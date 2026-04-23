import moment from "moment-timezone";
import { XMLParser } from "fast-xml-parser";
// @ts-ignore - adm-zip does not have type definitions in this project
import AdmZip from "adm-zip";
import { booleanPointInPolygon, point } from "@turf/turf";
import {
  Alert,
  IngestionConfig,
  IngestionResult,
  defaultLogger,
  hashDedupeKey,
  resolveFetch,
  resolveNow,
} from "nws-alerts-ingest-core";

const WPC_KML_BASE_URL = "https://www.wpc.ncep.noaa.gov/pwpf/latest_kml_GE";

interface SnowForecast {
  period: string;
  threshold: string;
  forecastHour: string;
  probability?: number;
}

interface PercentileForecast {
  period: string;
  percentile: number;
  forecastHour: string;
  accumulation?: number;
}

type Placemark = {
  name?: string;
  probability?: number | null;
  coordinates?: number[][];
};

const PROBABILITY_CHECKS = [
  {
    period: "12h",
    threshold: "2in",
    hours: [12, 18, 24, 30, 36, 42, 48, 54, 60, 66, 72],
  },
  {
    period: "24h",
    threshold: "1in",
    hours: [24, 30, 36, 42, 48, 54, 60, 66, 72],
  },
  { period: "48h", threshold: "2in", hours: [48, 54, 60, 66, 72] },
  { period: "72h", threshold: "4in", hours: [72] },
] as const;

const parseCoordinates = (coordString: string): number[][] => {
  const coords: number[][] = [];
  const lines = coordString.trim().split(/\s+/);
  for (const line of lines) {
    const parts = line.split(",");
    if (parts.length >= 2) {
      const lon = parseFloat(parts[0]);
      const lat = parseFloat(parts[1]);
      if (!Number.isNaN(lon) && !Number.isNaN(lat)) {
        coords.push([lon, lat]);
      }
    }
  }
  return coords;
};

const parseKML = (kmlContent: string): Placemark[] => {
  const parser = new XMLParser({
    ignoreAttributes: false,
    attributeNamePrefix: "@_",
  });
  const result = parser.parse(kmlContent);
  const placemarks: Placemark[] = [];

  try {
    const kml = result.kml || result;
    const document = kml.Document || kml;
    const folders = Array.isArray(document.Folder)
      ? document.Folder
      : [document.Folder];

    for (const folder of folders) {
      if (!folder) continue;
      const folderPlacemarks = Array.isArray(folder.Placemark)
        ? folder.Placemark
        : folder.Placemark
          ? [folder.Placemark]
          : [];

      for (const placemark of folderPlacemarks) {
        if (!placemark) continue;

        const name = placemark.name || "";
        const probMatch = name.match(/(\d+)%/);
        const probability = probMatch ? parseInt(probMatch[1], 10) : null;

        if (placemark.Polygon?.outerBoundaryIs?.LinearRing?.coordinates) {
          const coordString =
            placemark.Polygon.outerBoundaryIs.LinearRing.coordinates;
          const coords = parseCoordinates(coordString);
          placemarks.push({ name, probability, coordinates: coords });
        } else if (placemark.MultiGeometry?.Polygon) {
          const polygons = Array.isArray(placemark.MultiGeometry.Polygon)
            ? placemark.MultiGeometry.Polygon
            : [placemark.MultiGeometry.Polygon];
          for (const polygon of polygons) {
            if (polygon?.outerBoundaryIs?.LinearRing?.coordinates) {
              const coordString =
                polygon.outerBoundaryIs.LinearRing.coordinates;
              const coords = parseCoordinates(coordString);
              placemarks.push({ name, probability, coordinates: coords });
            }
          }
        }
      }
    }
  } catch (e) {
    // ignore parse errors and return what we have
  }

  return placemarks;
};

const extractKMLFromKMZ = (kmzBuffer: Buffer): string | null => {
  try {
    const zip = new AdmZip(kmzBuffer);
    const zipEntries = zip.getEntries();
    for (const entry of zipEntries) {
      if (entry.entryName.endsWith(".kml")) {
        return entry.getData().toString("utf8");
      }
    }
    return null;
  } catch {
    return null;
  }
};

const getLocationProbability = (
  placemarks: Placemark[],
  lat: number,
  lon: number,
): number | null => {
  const myPoint = point([lon, lat]);
  let highestProb: number | null = null;

  for (const placemark of placemarks) {
    if (!placemark.coordinates || placemark.coordinates.length < 3) continue;

    const coords = [...placemark.coordinates];
    const first = coords[0];
    const last = coords[coords.length - 1];
    if (first[0] !== last[0] || first[1] !== last[1]) {
      coords.push(first);
    }

    const polygon = {
      type: "Polygon" as const,
      coordinates: [coords],
    };

    if (booleanPointInPolygon(myPoint, polygon)) {
      if (placemark.probability && placemark.probability > (highestProb ?? 0)) {
        highestProb = placemark.probability;
      }
    }
  }

  return highestProb;
};

const buildProbabilityProductId = (
  period: string,
  threshold: string,
  forecastHour: string,
): string => {
  const periodNum = period.replace("h", "");
  const thresholdCode = threshold.replace("in", "").padStart(2, "0");
  return `prb_${periodNum}hsnow_ge${thresholdCode}_${forecastHour}_latest_GE`;
};

const buildPercentileProductId = (
  period: string,
  percentile: number,
  forecastHour: string,
): string => {
  const periodNum = period.replace("h", "");
  const pctlStr = percentile.toString().padStart(2, "0");
  return `percentile_${periodNum}hsnow${pctlStr}_${forecastHour}_latest_GE`;
};

type FetchKmlContentResult =
  | { kind: "content"; content: string }
  | { kind: "not-found" }
  | { kind: "unavailable" };

const fetchKmlContent = async (
  fetchImpl: typeof fetch,
  baseUrl: string,
): Promise<FetchKmlContentResult> => {
  const response = await fetchImpl(baseUrl);
  if (response.status === 404) {
    return { kind: "not-found" };
  }
  if (!response.ok) {
    return { kind: "unavailable" };
  }

  const buffer = Buffer.from(await response.arrayBuffer());
  const maybeKml = extractKMLFromKMZ(buffer);
  if (maybeKml) {
    return { kind: "content", content: maybeKml };
  }

  const asText = buffer.toString("utf8");
  if (asText.trim().startsWith("<")) {
    return { kind: "content", content: asText };
  }

  return { kind: "unavailable" };
};

const fetchWpcKmlContent = async (
  fetchImpl: typeof fetch,
  productId: string,
): Promise<string | null> => {
  const kmzUrl = `${WPC_KML_BASE_URL}/${productId}.kmz`;

  try {
    const kmzResult = await fetchKmlContent(fetchImpl, kmzUrl);
    if (kmzResult.kind === "not-found") {
      return null;
    }
    if (kmzResult.kind === "content") {
      return kmzResult.content;
    }
  } catch {
    // Fall back to .kml once for non-404 transport failures.
  }

  const kmlUrl = `${WPC_KML_BASE_URL}/${productId}.kml`;
  try {
    const kmlResult = await fetchKmlContent(fetchImpl, kmlUrl);
    return kmlResult.kind === "content" ? kmlResult.content : null;
  } catch {
    return null;
  }
};

const checkProbabilityKML = async (
  fetchImpl: typeof fetch,
  lat: number,
  lon: number,
  period: string,
  threshold: string,
  forecastHour: string,
): Promise<SnowForecast | null> => {
  const productId = buildProbabilityProductId(period, threshold, forecastHour);
  const kmlContent = await fetchWpcKmlContent(fetchImpl, productId);
  if (!kmlContent) {
    return null;
  }

  const placemarks = parseKML(kmlContent);
  const probability = getLocationProbability(placemarks, lat, lon);

  if (probability !== null && probability > 0) {
    return {
      period,
      threshold,
      forecastHour,
      probability,
    };
  }
  return null;
};

const checkPercentileKML = async (
  fetchImpl: typeof fetch,
  lat: number,
  lon: number,
  period: string,
  percentile: number,
  forecastHour: string,
): Promise<PercentileForecast | null> => {
  const productId = buildPercentileProductId(period, percentile, forecastHour);
  const kmlContent = await fetchWpcKmlContent(fetchImpl, productId);
  if (!kmlContent) {
    return null;
  }

  const placemarks = parseKML(kmlContent);
  const myPoint = point([lon, lat]);

  for (const placemark of placemarks) {
    if (!placemark.coordinates || placemark.coordinates.length < 3) continue;

    const coords = [...placemark.coordinates];
    const first = coords[0];
    const last = coords[coords.length - 1];
    if (first[0] !== last[0] || first[1] !== last[1]) {
      coords.push(first);
    }

    const polygon = {
      type: "Polygon" as const,
      coordinates: [coords],
    };

    if (booleanPointInPolygon(myPoint, polygon)) {
      const accMatch = placemark.name?.match(/(\d+\.?\d*)/);
      const accumulation = accMatch ? parseFloat(accMatch[1]) : null;
      if (accumulation !== null && accumulation > 0) {
        return {
          period,
          percentile,
          forecastHour,
          accumulation,
        };
      }
    }
  }

  return null;
};

const checkSnowForecasts = async (
  fetchImpl: typeof fetch,
  lat: number,
  lon: number,
): Promise<{
  forecasts: SnowForecast[];
  percentileForecasts: PercentileForecast[];
}> => {
  const forecasts: SnowForecast[] = [];
  const percentileForecasts: PercentileForecast[] = [];

  for (const check of PROBABILITY_CHECKS) {
    for (const hour of check.hours) {
      const forecastHour = `f${hour.toString().padStart(3, "0")}`;
      const result = await checkProbabilityKML(
        fetchImpl,
        lat,
        lon,
        check.period,
        check.threshold,
        forecastHour,
      );
      if (result) {
        forecasts.push(result);
      }
    }
  }

  const percentileChecks = [
    { period: "12h", hours: [12, 18, 24, 30, 36, 42, 48, 54, 60, 66, 72] },
    { period: "24h", hours: [24, 30, 36, 42, 48, 54, 60, 66, 72] },
    { period: "48h", hours: [48, 54, 60, 66, 72] },
    { period: "72h", hours: [72] },
  ];

  for (const check of percentileChecks) {
    for (const hour of check.hours) {
      const forecastHour = `f${hour.toString().padStart(3, "0")}`;
      const result = await checkPercentileKML(
        fetchImpl,
        lat,
        lon,
        check.period,
        50,
        forecastHour,
      );
      if (result && result.accumulation && result.accumulation >= 0.1) {
        percentileForecasts.push(result);
      }
    }
  }

  return { forecasts, percentileForecasts };
};

const buildSnowShortDescription = (summary: {
  forecasts: Array<{
    period: string;
    threshold: string;
    forecastHour: string;
    probability?: number;
  }>;
  percentiles: Array<{
    period: string;
    forecastHour: string;
    accumulation?: number;
  }>;
}): string => {
  const probabilityLines = summary.forecasts.slice(0, 3).map((forecast) => {
    const probabilityText =
      forecast.probability == null ? "n/a" : `${forecast.probability}%`;
    return `${forecast.period} ≥${forecast.threshold} (${forecast.forecastHour}): ${probabilityText}`;
  });

  const percentileLines = summary.percentiles.slice(0, 3).map((percentile) => {
    const accumulationText =
      percentile.accumulation == null ? "n/a" : `${percentile.accumulation}\"`;
    return `${percentile.period} 50th (${percentile.forecastHour}): ${accumulationText}`;
  });

  const segments = [
    "WPC Probabilistic Winter Precipitation Forecast.",
    probabilityLines.length
      ? `Probability outlooks: ${probabilityLines.join("; ")}.`
      : undefined,
    percentileLines.length
      ? `Median accumulations: ${percentileLines.join("; ")}.`
      : undefined,
  ].filter((segment): segment is string => Boolean(segment));

  return segments.join(" ").replace(/\s+/g, " ").trim();
};

/**
 * Ingests WPC probabilistic winter precipitation forecast products for a location.
 *
 * @param config - Runtime ingestion configuration with location and optional overrides.
 * @returns Canonical alerts, raw probability/percentile datasets, metadata, and dedupe keys.
 */
export const ingest = async (
  config: IngestionConfig,
): Promise<IngestionResult<Record<string, unknown>>> => {
  const logger = config.logger ?? defaultLogger;
  const fetchImpl = resolveFetch(config);
  const now = moment(resolveNow(config)).utc();
  const sourceLocation = { lat: config.lat, lon: config.lon };

  const { forecasts, percentileForecasts } = await checkSnowForecasts(
    fetchImpl,
    sourceLocation.lat,
    sourceLocation.lon,
  );

  const alerts: Alert[] = [];
  if (forecasts.length > 0 || percentileForecasts.length > 0) {
    const summary = {
      forecasts: forecasts.map((f) => ({
        period: f.period,
        threshold: f.threshold,
        forecastHour: f.forecastHour,
        probability: f.probability,
      })),
      percentiles: percentileForecasts.map((p) => ({
        period: p.period,
        forecastHour: p.forecastHour,
        accumulation: p.accumulation,
      })),
    };

    alerts.push({
      nwsId: hashDedupeKey(JSON.stringify(summary)),
      event: "WPC Snow Forecast",
      headline: `WPC Snow Forecast - ${forecasts.length} probability alerts, ${percentileForecasts.length} percentile alerts`,
      description: "WPC Probabilistic Winter Precipitation Forecast",
      shortDescription: buildSnowShortDescription(summary),
      sent: new Date(now.toISOString()),
      source: "wpc-snow-pwpf",
      extra: {
        ...summary,
        location: sourceLocation,
      },
    });
  }

  logger.info("wpc-snow-pwpf:alerts", { count: alerts.length });

  return {
    alerts,
    raw: { forecasts, percentileForecasts },
    meta: { issuedAt: now.toISOString(), sourceLocation },
    dedupeKeys: alerts.map((alert) => alert.nwsId),
  };
};
