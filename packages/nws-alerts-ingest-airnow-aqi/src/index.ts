import {
  Alert,
  IngestionConfig,
  IngestionResult,
  defaultLogger,
  hashDedupeKey,
  resolveFetch,
  resolveNow,
} from "nws-alerts-ingest-core";

const AIRNOW_OBSERVATION_URL =
  "https://www.airnowapi.org/aq/observation/latLong/current/";
const AIRNOW_FORECAST_URL = "https://www.airnowapi.org/aq/forecast/latLong/";

type AirNowCategory = {
  Number?: number;
  Name?: string;
};

export type AirNowObservationRecord = {
  DateObserved?: string;
  HourObserved?: number;
  LocalTimeZone?: string;
  ReportingArea?: string;
  StateCode?: string;
  Latitude?: number;
  Longitude?: number;
  ParameterName?: string;
  AQI?: number;
  Category?: AirNowCategory;
};

export type AirNowForecastRecord = {
  DateIssue?: string;
  DateForecast?: string;
  ReportingArea?: string;
  StateCode?: string;
  Latitude?: number;
  Longitude?: number;
  ParameterName?: string;
  AQI?: number;
  Category?: AirNowCategory;
  ActionDay?: boolean;
  Discussion?: string;
};

export type AirNowAqiExtra = {
  kind: "observed" | "forecast";
  aqi: number;
  categoryNumber?: number;
  categoryName?: string;
  dominantPollutant?: string;
  reportingArea: string;
  stateCode?: string;
  latitude?: number;
  longitude?: number;
  distanceMiles?: number;
  dateObserved?: string;
  hourObserved?: number;
  localTimeZone?: string;
  observedAt?: string;
  dateIssue?: string;
  dateForecast?: string;
  actionDay?: boolean;
  discussion?: string;
};

export type AirNowAqiConfig = IngestionConfig & {
  apiKey: string;
  distanceMiles?: number;
  forecastLookaheadDays?: number;
};

type AreaLike = {
  ReportingArea?: string;
  StateCode?: string;
  Latitude?: number;
  Longitude?: number;
};

const toFiniteNumber = (value: unknown): number | undefined => {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }

  return undefined;
};

const toTrimmedString = (value: unknown): string | undefined => {
  if (typeof value !== "string") {
    return undefined;
  }

  const trimmed = value.trim();
  return trimmed.length ? trimmed : undefined;
};

const toIsoDateString = (value: unknown): string | undefined => {
  const text = toTrimmedString(value);
  if (!text) {
    return undefined;
  }

  const parsed = Date.parse(text);
  if (Number.isFinite(parsed)) {
    return new Date(parsed).toISOString();
  }

  const simpleDate = `${text}T00:00:00.000Z`;
  const simpleParsed = Date.parse(simpleDate);
  if (Number.isFinite(simpleParsed)) {
    return new Date(simpleParsed).toISOString();
  }

  return undefined;
};

const formatDateOnly = (value: Date): string => {
  return value.toISOString().slice(0, 10);
};

const toAreaKey = (item: AreaLike): string => {
  const reportingArea = toTrimmedString(item.ReportingArea) ?? "unknown-area";
  const stateCode = toTrimmedString(item.StateCode) ?? "unknown-state";
  const latitude = toFiniteNumber(item.Latitude);
  const longitude = toFiniteNumber(item.Longitude);
  return [
    reportingArea,
    stateCode,
    latitude?.toFixed(4) ?? "unknown-lat",
    longitude?.toFixed(4) ?? "unknown-lon",
  ].join("|");
};

const toRadians = (value: number): number => (value * Math.PI) / 180;

const distanceMilesBetween = (
  lat1: number,
  lon1: number,
  lat2: number,
  lon2: number,
): number => {
  const earthRadiusMiles = 3958.8;
  const dLat = toRadians(lat2 - lat1);
  const dLon = toRadians(lon2 - lon1);
  const a =
    Math.sin(dLat / 2) * Math.sin(dLat / 2) +
    Math.cos(toRadians(lat1)) *
      Math.cos(toRadians(lat2)) *
      Math.sin(dLon / 2) *
      Math.sin(dLon / 2);
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  return earthRadiusMiles * c;
};

const selectNearestAreaRecords = <T extends AreaLike>(
  items: T[],
  lat: number,
  lon: number,
): { records: T[]; distanceMiles?: number } => {
  if (!items.length) {
    return { records: [] };
  }

  const grouped = new Map<string, T[]>();
  for (const item of items) {
    const key = toAreaKey(item);
    const existing = grouped.get(key);
    if (existing) {
      existing.push(item);
      continue;
    }
    grouped.set(key, [item]);
  }

  let selectedRecords: T[] = [];
  let selectedDistance = Number.POSITIVE_INFINITY;
  for (const records of grouped.values()) {
    const sample = records[0];
    const sampleLat = toFiniteNumber(sample.Latitude);
    const sampleLon = toFiniteNumber(sample.Longitude);
    const distance =
      sampleLat != null && sampleLon != null
        ? distanceMilesBetween(lat, lon, sampleLat, sampleLon)
        : Number.POSITIVE_INFINITY;
    if (distance < selectedDistance) {
      selectedDistance = distance;
      selectedRecords = records;
    }
  }

  return {
    records: selectedRecords,
    distanceMiles: Number.isFinite(selectedDistance)
      ? selectedDistance
      : undefined,
  };
};

const getHighestAqiRecord = <T extends { AQI?: number }>(items: T[]): T | undefined => {
  return items.reduce<T | undefined>((best, current) => {
    const currentAqi = toFiniteNumber(current.AQI);
    if (currentAqi == null) {
      return best;
    }

    if (!best) {
      return current;
    }

    const bestAqi = toFiniteNumber(best.AQI) ?? Number.NEGATIVE_INFINITY;
    return currentAqi > bestAqi ? current : best;
  }, undefined);
};

const buildObservedTimestamp = (record: AirNowObservationRecord): string | undefined => {
  const dateObserved = toTrimmedString(record.DateObserved);
  const hourObserved = toFiniteNumber(record.HourObserved);
  if (!dateObserved || hourObserved == null) {
    return undefined;
  }

  const hour = Math.max(0, Math.min(23, Math.trunc(hourObserved)));
  const paddedHour = hour.toString().padStart(2, "0");
  return `${dateObserved}T${paddedHour}:00:00.000Z`;
};

const buildObservedAlert = (
  record: AirNowObservationRecord,
  distanceMiles?: number,
): Alert | undefined => {
  const aqi = toFiniteNumber(record.AQI);
  const reportingArea = toTrimmedString(record.ReportingArea);
  if (aqi == null || !reportingArea) {
    return undefined;
  }

  const categoryNumber = toFiniteNumber(record.Category?.Number);
  const categoryName = toTrimmedString(record.Category?.Name);
  const parameterName = toTrimmedString(record.ParameterName);
  const stateCode = toTrimmedString(record.StateCode);
  const observedAt = buildObservedTimestamp(record);
  const locationLabel = stateCode ? `${reportingArea}, ${stateCode}` : reportingArea;
  const dominantPollutant = parameterName ?? "Unknown pollutant";

  return {
    nwsId: [
      "airnow-observed",
      reportingArea.toLowerCase().replace(/[^a-z0-9]+/g, "-"),
      stateCode?.toLowerCase() ?? "na",
      observedAt ?? toTrimmedString(record.DateObserved) ?? "unknown-date",
      dominantPollutant.toLowerCase().replace(/[^a-z0-9]+/g, "-"),
    ].join("|"),
    event: "AirNow AQI Threshold Alert",
    headline: `Current AQI ${aqi} (${categoryName ?? "Unknown"}) for ${locationLabel}`,
    description: [
      `Current AirNow AQI for ${locationLabel} is ${aqi}${
        categoryName ? ` (${categoryName})` : ""
      }.`,
      `Dominant pollutant: ${dominantPollutant}.`,
      observedAt ? `Observed at: ${observedAt}.` : undefined,
    ]
      .filter(Boolean)
      .join(" "),
    shortDescription: `AQI ${aqi}${categoryName ? ` (${categoryName})` : ""} in ${locationLabel} due to ${dominantPollutant}.`,
    sent: observedAt ? new Date(observedAt) : undefined,
    effective: observedAt ? new Date(observedAt) : undefined,
    onset: observedAt ? new Date(observedAt) : undefined,
    source: "airnow-aqi",
    extra: {
      kind: "observed",
      aqi,
      categoryNumber,
      categoryName,
      dominantPollutant,
      reportingArea,
      stateCode,
      latitude: toFiniteNumber(record.Latitude),
      longitude: toFiniteNumber(record.Longitude),
      distanceMiles,
      dateObserved: toTrimmedString(record.DateObserved),
      hourObserved: toFiniteNumber(record.HourObserved),
      localTimeZone: toTrimmedString(record.LocalTimeZone),
      observedAt,
    } satisfies AirNowAqiExtra,
  };
};

const buildForecastAlert = (
  record: AirNowForecastRecord,
  distanceMiles?: number,
): Alert | undefined => {
  const aqi = toFiniteNumber(record.AQI);
  const reportingArea = toTrimmedString(record.ReportingArea);
  const forecastDate = toTrimmedString(record.DateForecast);
  if (aqi == null || !reportingArea || !forecastDate) {
    return undefined;
  }

  const categoryNumber = toFiniteNumber(record.Category?.Number);
  const categoryName = toTrimmedString(record.Category?.Name);
  const parameterName = toTrimmedString(record.ParameterName);
  const stateCode = toTrimmedString(record.StateCode);
  const issuedAt = toIsoDateString(record.DateIssue);
  const effectiveAt = toIsoDateString(record.DateForecast);
  const effectiveDate = effectiveAt ? new Date(effectiveAt) : undefined;
  const expiresDate = effectiveDate
    ? new Date(effectiveDate.getTime() + 24 * 60 * 60 * 1000)
    : undefined;
  const locationLabel = stateCode ? `${reportingArea}, ${stateCode}` : reportingArea;
  const dominantPollutant = parameterName ?? "Unknown pollutant";

  return {
    nwsId: [
      "airnow-forecast",
      reportingArea.toLowerCase().replace(/[^a-z0-9]+/g, "-"),
      stateCode?.toLowerCase() ?? "na",
      forecastDate,
      dominantPollutant.toLowerCase().replace(/[^a-z0-9]+/g, "-"),
    ].join("|"),
    event: "AirNow AQI Forecast Alert",
    headline: `Forecast AQI ${aqi} (${categoryName ?? "Unknown"}) for ${forecastDate} in ${locationLabel}`,
    description: [
      `Forecast AirNow AQI for ${locationLabel} on ${forecastDate} is ${aqi}${
        categoryName ? ` (${categoryName})` : ""
      }.`,
      `Dominant pollutant: ${dominantPollutant}.`,
      issuedAt ? `Forecast issued at: ${issuedAt}.` : undefined,
      toTrimmedString(record.Discussion),
    ]
      .filter(Boolean)
      .join(" "),
    shortDescription: `Forecast AQI ${aqi}${categoryName ? ` (${categoryName})` : ""} in ${locationLabel} for ${forecastDate}.`,
    sent: issuedAt ? new Date(issuedAt) : undefined,
    effective: effectiveDate,
    onset: effectiveDate,
    expires: expiresDate,
    ends: expiresDate,
    source: "airnow-aqi",
    extra: {
      kind: "forecast",
      aqi,
      categoryNumber,
      categoryName,
      dominantPollutant,
      reportingArea,
      stateCode,
      latitude: toFiniteNumber(record.Latitude),
      longitude: toFiniteNumber(record.Longitude),
      distanceMiles,
      dateIssue: toTrimmedString(record.DateIssue),
      dateForecast: forecastDate,
      actionDay: record.ActionDay === true,
      discussion: toTrimmedString(record.Discussion),
    } satisfies AirNowAqiExtra,
  };
};

const buildRequestUrl = (
  baseUrl: string,
  params: Record<string, string | number>,
): string => {
  const url = new URL(baseUrl);
  for (const [key, value] of Object.entries(params)) {
    url.searchParams.set(key, String(value));
  }
  return url.toString();
};

const fetchJson = async <T>(
  fetchImpl: typeof fetch,
  url: string,
  userAgent: string,
): Promise<T> => {
  const response = await fetchImpl(url, {
    headers: {
      "User-Agent": userAgent,
    },
  });

  if (!response.ok) {
    throw new Error(`airnow-aqi: fetch failed ${response.status} for ${url}`);
  }

  return (await response.json()) as T;
};

export const ingest = async (
  config: AirNowAqiConfig,
): Promise<IngestionResult<{ observed: AirNowObservationRecord[]; forecasts: AirNowForecastRecord[] }>> => {
  if (!toTrimmedString(config.apiKey)) {
    throw new Error("airnow-aqi: apiKey is required");
  }

  const logger = config.logger ?? defaultLogger;
  const fetchImpl = resolveFetch(config);
  const now = resolveNow(config);
  const distanceMiles = Math.max(1, Math.trunc(config.distanceMiles ?? 25));
  const forecastLookaheadDays = Math.max(
    1,
    Math.min(5, Math.trunc(config.forecastLookaheadDays ?? 2)),
  );
  const userAgent =
    config.userAgent ?? "nws-alerts-ingest-airnow-aqi/0.1.0";
  const observedUrl = buildRequestUrl(AIRNOW_OBSERVATION_URL, {
    format: "application/json",
    latitude: config.lat,
    longitude: config.lon,
    distance: distanceMiles,
    API_KEY: config.apiKey,
  });

  const forecastUrls = Array.from({ length: forecastLookaheadDays }, (_, offset) => {
    const date = new Date(now.getTime() + offset * 24 * 60 * 60 * 1000);
    return buildRequestUrl(AIRNOW_FORECAST_URL, {
      format: "application/json",
      latitude: config.lat,
      longitude: config.lon,
      distance: distanceMiles,
      date: formatDateOnly(date),
      API_KEY: config.apiKey,
    });
  });

  const rawObserved = await fetchJson<AirNowObservationRecord[]>(
    fetchImpl,
    observedUrl,
    userAgent,
  );

  const rawForecastResponses = await Promise.all(
    forecastUrls.map((url) => fetchJson<AirNowForecastRecord[]>(fetchImpl, url, userAgent)),
  );
  const rawForecasts = rawForecastResponses.flat();

  const { records: nearestObservationRecords, distanceMiles: observedDistance } =
    selectNearestAreaRecords(rawObserved, config.lat, config.lon);
  const observedAlert = buildObservedAlert(
    getHighestAqiRecord(nearestObservationRecords) ?? {},
    observedDistance,
  );

  const { records: nearestForecastRecords, distanceMiles: forecastDistance } =
    selectNearestAreaRecords(rawForecasts, config.lat, config.lon);
  const forecastsByDate = new Map<string, AirNowForecastRecord[]>();
  for (const record of nearestForecastRecords) {
    const dateForecast = toTrimmedString(record.DateForecast);
    if (!dateForecast) {
      continue;
    }
    const existing = forecastsByDate.get(dateForecast);
    if (existing) {
      existing.push(record);
      continue;
    }
    forecastsByDate.set(dateForecast, [record]);
  }

  const forecastAlerts = [...forecastsByDate.entries()]
    .sort((left, right) => left[0].localeCompare(right[0]))
    .map(([, records]) => buildForecastAlert(getHighestAqiRecord(records) ?? {}, forecastDistance))
    .filter((alert): alert is Alert => Boolean(alert));

  const alerts = [observedAlert, ...forecastAlerts].filter(
    (alert): alert is Alert => Boolean(alert),
  );

  logger.info("airnow-aqi:ingest", {
    observedCount: rawObserved.length,
    forecastCount: rawForecasts.length,
    alertCount: alerts.length,
  });

  return {
    alerts,
    raw: {
      observed: rawObserved,
      forecasts: rawForecasts,
    },
    meta: {
      observedUrl,
      forecastUrls,
      observedCount: rawObserved.length,
      forecastCount: rawForecasts.length,
    },
    dedupeKeys: alerts.map((alert) => hashDedupeKey(alert.nwsId)),
  };
};