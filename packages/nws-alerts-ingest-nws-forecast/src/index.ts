import {
  Alert,
  buildShortDescriptionFromDescription,
  IngestionConfig,
  IngestionResult,
  defaultLogger,
  hashDedupeKey,
  resolveFetch,
  toIso,
} from "nws-alerts-ingest-core";

/**
 * Forecast endpoint family resolved from NWS `points` metadata.
 *
 * - `forecast`: 7-day/daypart textual forecast periods.
 * - `forecastHourly`: hourly forecast periods.
 * - `forecastGridData`: gridded time-series parameters.
 */
export type ForecastType = "forecast" | "forecastHourly" | "forecastGridData";

/**
 * Shape of raw payload attached to `IngestionResult.raw`.
 *
 * - `full`: full upstream NWS response JSON.
 * - `compact`: reduced, columnar-friendly representation.
 */
export type ForecastRawMode = "full" | "compact";

/**
 * Runtime configuration for forecast ingestion.
 */
export type ForecastIngestionConfig = IngestionConfig & {
  /** Forecast endpoint variant to fetch; defaults to `forecast`. */
  forecastType?: ForecastType;
  /** Raw payload mode when `includeRaw` is enabled; defaults to `full`. */
  rawMode?: ForecastRawMode;
  /** Optional limit for period/row compaction, clamped to 1..240. */
  maxPeriods?: number;
  /** Include upstream raw payload in the ingestion result; defaults to `true`. */
  includeRaw?: boolean;
  /** Grid fields to omit from compact grid output. */
  compactGridFields?: string[];
};

type PointsResponse = {
  properties?: {
    forecast?: string;
    forecastHourly?: string;
    forecastGridData?: string;
    [key: string]: unknown;
  };
};

type ForecastJson = Record<string, unknown>;

const POINTS_BASE_URL = "https://api.weather.gov/points";
const DEFAULT_FORECAST_TYPE: ForecastType = "forecast";
const DEFAULT_RAW_MODE: ForecastRawMode = "full";
const DEFAULT_MAX_PERIODS = 240;

const validateUserAgent = (userAgent?: string): string => {
  const trimmed = userAgent?.trim();
  if (!trimmed) {
    throw new Error(
      "nws-forecast: userAgent is required by NWS API policy. Provide config.userAgent with app name and contact info.",
    );
  }
  return trimmed;
};

const getForecastUrlFromPoints = (
  points: PointsResponse,
  forecastType: ForecastType,
): string => {
  const forecastUrl = points.properties?.[forecastType];
  if (typeof forecastUrl !== "string" || forecastUrl.length === 0) {
    throw new Error(
      `nws-forecast: points response missing properties.${forecastType}`,
    );
  }
  return forecastUrl;
};

const normalizeMaxPeriods = (value?: number): number | undefined => {
  if (!Number.isFinite(value)) {
    return undefined;
  }
  const parsed = Math.floor(value as number);
  if (parsed < 1) {
    return 1;
  }
  if (parsed > DEFAULT_MAX_PERIODS) {
    return DEFAULT_MAX_PERIODS;
  }
  return parsed;
};

const sliceByLimit = <T>(items: T[], maxItems?: number): T[] => {
  if (!Number.isFinite(maxItems)) {
    return items;
  }
  return items.slice(0, maxItems as number);
};

const isUnitCodeValueObject = (
  value: unknown,
): value is { unitCode: unknown; value: unknown } => {
  if (!value || typeof value !== "object") {
    return false;
  }

  const row = value as Record<string, unknown>;
  const keys = Object.keys(row);
  return (
    keys.length === 2 &&
    Object.prototype.hasOwnProperty.call(row, "unitCode") &&
    Object.prototype.hasOwnProperty.call(row, "value")
  );
};

const toColumnarRecords = (
  records: unknown,
  maxItems?: number,
  excludedFields: string[] = [],
): Record<string, unknown> => {
  if (!Array.isArray(records)) {
    return {
      encoding: "columnar-v1",
      rowCount: 0,
      fields: [],
      columns: {},
    };
  }

  const rows = sliceByLimit(records, maxItems) as Record<string, unknown>[];
  const fields: string[] = [];
  const fieldSet = new Set<string>();

  for (const row of rows) {
    for (const key of Object.keys(row)) {
      if (!fieldSet.has(key)) {
        fieldSet.add(key);
        fields.push(key);
      }
    }
  }

  const excludedFieldSet = new Set(excludedFields);
  const filteredFields = fields.filter((field) => !excludedFieldSet.has(field));

  const columns: Record<string, unknown[]> = {};
  const columnMeta: Record<string, Record<string, unknown>> = {};
  for (const field of filteredFields) {
    const rawValues = rows.map((row) => {
      const value = row[field];
      return value === undefined ? null : value;
    });

    const nonNullValues = rawValues.filter((value) => value !== null);
    const canNormalizeUnitCode =
      nonNullValues.length > 0 &&
      nonNullValues.every((value) => isUnitCodeValueObject(value));

    if (canNormalizeUnitCode) {
      const unitCodes = new Set(
        nonNullValues.map((value) =>
          String((value as { unitCode: unknown }).unitCode),
        ),
      );

      if (unitCodes.size === 1) {
        const [unitCode] = [...unitCodes];
        columnMeta[field] = { unitCode };
        columns[field] = rawValues.map((value) =>
          value === null
            ? null
            : (value as { unitCode: unknown; value: unknown }).value,
        );
        continue;
      }
    }

    columns[field] = rawValues;
  }

  return {
    encoding: "columnar-v1",
    rowCount: rows.length,
    fields: filteredFields,
    columnMeta,
    columns,
  };
};

const normalizeWindSpeedUnit = (data: Record<string, unknown>): void => {
  const fields = ((data.fields ?? []) as string[]).slice();
  const columns = { ...((data.columns ?? {}) as Record<string, unknown[]>) };
  const columnMeta = {
    ...((data.columnMeta ?? {}) as Record<string, Record<string, unknown>>),
  };

  const windSpeed = columns.windSpeed;
  if (!fields.includes("windSpeed") || !Array.isArray(windSpeed)) {
    data.fields = fields;
    data.columns = columns;
    data.columnMeta = columnMeta;
    return;
  }

  const parser = /^(.+?)\s*(mph|km\/h|kph|kt|kts|m\/s)$/i;
  const numericParser = /-?\d+(?:\.\d+)?/;
  const parsed = windSpeed.map((entry) => {
    if (entry === null || entry === undefined) {
      return {
        raw: entry,
        value: null as number | null,
        unit: undefined as string | undefined,
      };
    }
    if (typeof entry === "number" && Number.isFinite(entry)) {
      return {
        raw: entry,
        value: entry,
        unit: undefined as string | undefined,
      };
    }
    if (typeof entry !== "string") {
      return {
        raw: entry,
        value: null as number | null,
        unit: undefined as string | undefined,
      };
    }
    const match = entry.trim().match(parser);
    const valueText = match ? match[1].trim() : entry.trim();
    const numberMatch = valueText.match(numericParser);
    const numericValue = numberMatch ? Number(numberMatch[0]) : null;

    return {
      raw: entry,
      value: Number.isFinite(numericValue) ? numericValue : null,
      unit: match?.[2]?.toLowerCase(),
    };
  });

  const units = parsed
    .map((item) => item.unit)
    .filter((unit): unit is string => !!unit);

  const uniqueUnits = [...new Set(units)];
  if (uniqueUnits.length === 1) {
    const [unitCode] = uniqueUnits;
    columnMeta.windSpeed = {
      ...(columnMeta.windSpeed || {}),
      unitCode,
    };
  }

  columns.windSpeed = parsed.map((item) => item.value);

  data.fields = fields;
  data.columns = columns;
  data.columnMeta = columnMeta;
};

const normalizeIconPrefix = (data: Record<string, unknown>): void => {
  const fields = ((data.fields ?? []) as string[]).slice();
  const columns = { ...((data.columns ?? {}) as Record<string, unknown[]>) };
  const columnMeta = {
    ...((data.columnMeta ?? {}) as Record<string, Record<string, unknown>>),
  };

  const iconValues = columns.icon;
  if (!fields.includes("icon") || !Array.isArray(iconValues)) {
    data.fields = fields;
    data.columns = columns;
    data.columnMeta = columnMeta;
    return;
  }

  const nonNullIcons = iconValues.filter(
    (value): value is string => value !== null && value !== undefined,
  );

  if (
    nonNullIcons.length === 0 ||
    nonNullIcons.some((value) => typeof value !== "string")
  ) {
    data.fields = fields;
    data.columns = columns;
    data.columnMeta = columnMeta;
    return;
  }

  const commonPrefix = nonNullIcons.reduce((prefix, value) => {
    const max = Math.min(prefix.length, value.length);
    let index = 0;
    while (index < max && prefix[index] === value[index]) {
      index += 1;
    }
    return prefix.slice(0, index);
  });

  if (!commonPrefix || commonPrefix.length < 8) {
    data.fields = fields;
    data.columns = columns;
    data.columnMeta = columnMeta;
    return;
  }

  const slashIndex = commonPrefix.lastIndexOf("/");
  const normalizedPrefix =
    slashIndex > "https://".length
      ? commonPrefix.slice(0, slashIndex + 1)
      : commonPrefix;

  if (!normalizedPrefix || normalizedPrefix.length < 8) {
    data.fields = fields;
    data.columns = columns;
    data.columnMeta = columnMeta;
    return;
  }

  if (!nonNullIcons.every((value) => value.startsWith(normalizedPrefix))) {
    data.fields = fields;
    data.columns = columns;
    data.columnMeta = columnMeta;
    return;
  }

  columnMeta.icon = {
    ...(columnMeta.icon || {}),
    urlPrefix: normalizedPrefix,
  };

  columns.icon = iconValues.map((value) => {
    if (value === null || value === undefined) {
      return value;
    }
    if (typeof value !== "string") {
      return value;
    }
    return value.slice(normalizedPrefix.length);
  });

  data.fields = fields;
  data.columns = columns;
  data.columnMeta = columnMeta;
};

const normalizeDewpointRounding = (data: Record<string, unknown>): void => {
  const fields = ((data.fields ?? []) as string[]).slice();
  const columns = { ...((data.columns ?? {}) as Record<string, unknown[]>) };
  const columnMeta = {
    ...((data.columnMeta ?? {}) as Record<string, Record<string, unknown>>),
  };

  const dewpoint = columns.dewpoint;
  if (!fields.includes("dewpoint") || !Array.isArray(dewpoint)) {
    data.fields = fields;
    data.columns = columns;
    data.columnMeta = columnMeta;
    return;
  }

  columns.dewpoint = dewpoint.map((value) => {
    if (typeof value !== "number" || !Number.isFinite(value)) {
      return value;
    }
    return Math.round(value);
  });

  data.fields = fields;
  data.columns = columns;
  data.columnMeta = columnMeta;
};

const toNumericSeries = (values: unknown[]): Array<number | null> =>
  values.map((value) => {
    if (typeof value === "number" && Number.isFinite(value)) {
      return value;
    }
    if (typeof value === "string") {
      const match = value.match(/-?\d+(?:\.\d+)?/);
      if (match) {
        const parsed = Number(match[0]);
        if (Number.isFinite(parsed)) {
          return parsed;
        }
      }
    }
    return null;
  });

const summarizeNumericSeries = (
  values: Array<number | null>,
): {
  min?: number;
  max?: number;
  avg?: number;
  start?: number;
  end?: number;
  trend?: "rising" | "falling" | "steady";
} => {
  const present = values.filter((value): value is number => value !== null);
  if (present.length === 0) {
    return {};
  }

  const start = present[0];
  const end = present[present.length - 1];
  const avg = present.reduce((sum, value) => sum + value, 0) / present.length;
  const delta = end - start;
  const trend = delta > 1 ? "rising" : delta < -1 ? "falling" : "steady";

  return {
    min: Math.min(...present),
    max: Math.max(...present),
    avg: Math.round(avg * 10) / 10,
    start,
    end,
    trend,
  };
};

const extractRangeStarts = (ranges: unknown[]): Array<string | null> =>
  ranges.map((value) => {
    if (Array.isArray(value) && typeof value[0] === "string") {
      return value[0];
    }
    return null;
  });

const extractRangeEnds = (ranges: unknown[]): Array<string | null> =>
  ranges.map((value) => {
    if (Array.isArray(value) && typeof value[1] === "string") {
      return value[1];
    }
    return null;
  });

const toTimestamp = (value: string | null): number | null => {
  if (!value) {
    return null;
  }
  const parsed = Date.parse(value);
  return Number.isFinite(parsed) ? parsed : null;
};

const buildWindows = (
  indices: number[],
  starts: Array<string | null>,
  ends: Array<string | null>,
  values: Array<number | null>,
  threshold: number,
  type: string,
): Array<Record<string, unknown>> => {
  const windows: Array<Record<string, unknown>> = [];
  let windowStart: number | null = null;
  let peak = Number.NEGATIVE_INFINITY;

  const closeWindow = (endIndex: number) => {
    if (windowStart === null) {
      return;
    }
    windows.push({
      type,
      start: starts[windowStart],
      end: ends[endIndex],
      peak: Number.isFinite(peak) ? Math.round(peak * 10) / 10 : null,
    });
    windowStart = null;
    peak = Number.NEGATIVE_INFINITY;
  };

  for (let offset = 0; offset < indices.length; offset += 1) {
    const index = indices[offset];
    const value = values[index];
    const inWindow = value !== null && value >= threshold;

    if (inWindow) {
      if (windowStart === null) {
        windowStart = index;
      }
      peak = Math.max(peak, value);
      continue;
    }

    if (windowStart !== null) {
      const previousIndex = indices[Math.max(0, offset - 1)];
      closeWindow(previousIndex);
    }
  }

  if (windowStart !== null && indices.length > 0) {
    closeWindow(indices[indices.length - 1]);
  }

  return windows;
};

const buildHourlyDerivedSummary = (
  columnar: Record<string, unknown>,
): Record<string, unknown> => {
  const columns = (columnar.columns ?? {}) as Record<string, unknown[]>;
  const columnMeta = (columnar.columnMeta ?? {}) as Record<
    string,
    Record<string, unknown>
  >;
  const rowCount = Number(columnar.rowCount ?? 0);

  const ranges = (columns.range ?? []) as unknown[];
  const starts = extractRangeStarts(ranges);
  const ends = extractRangeEnds(ranges);
  const timestamps = starts.map((value) => toTimestamp(value));
  const anchor = timestamps.find((value): value is number => value !== null);
  const firstStart = starts.find((value): value is string => !!value) ?? null;
  const offsetMatch = firstStart?.match(/([+-]\d{2}:\d{2}|Z)$/);
  const timeOffset = offsetMatch?.[1] ?? "Z";
  const currentDayKey = firstStart?.slice(0, 10) ?? null;

  const temperature = toNumericSeries((columns.temperature ?? []) as unknown[]);
  const dewpoint = toNumericSeries((columns.dewpoint ?? []) as unknown[]);
  const windSpeed = toNumericSeries((columns.windSpeed ?? []) as unknown[]);
  const precipitationChance = toNumericSeries(
    (columns.probabilityOfPrecipitation ?? []) as unknown[],
  );

  const inHorizon = (hours: number) =>
    timestamps
      .map((value, index) => ({ value, index }))
      .filter(
        (entry): entry is { value: number; index: number } =>
          entry.value !== null &&
          anchor !== undefined &&
          entry.value <= anchor + hours * 60 * 60 * 1000,
      )
      .map((entry) => entry.index);

  const nextDayIndices = inHorizon(24);
  const weekIndices = inHorizon(7 * 24);

  const turningPoints: Array<Record<string, unknown>> = [];
  for (let index = 1; index < temperature.length - 1; index += 1) {
    const prev = temperature[index - 1];
    const curr = temperature[index];
    const next = temperature[index + 1];
    if (prev === null || curr === null || next === null) {
      continue;
    }
    if (curr > prev && curr > next) {
      turningPoints.push({
        type: "temperature_peak",
        at: starts[index],
        value: curr,
      });
    } else if (curr < prev && curr < next) {
      turningPoints.push({
        type: "temperature_valley",
        at: starts[index],
        value: curr,
      });
    }
  }

  const eventCandidates: Array<Record<string, unknown>> = [];

  const rainOnsetIndex = precipitationChance.findIndex(
    (value) => value !== null && value >= 30,
  );
  if (rainOnsetIndex >= 0) {
    eventCandidates.push({
      type: "rain_onset",
      priority: 90,
      at: starts[rainOnsetIndex],
      value: precipitationChance[rainOnsetIndex],
    });
  }

  const windPairs = windSpeed
    .map((value, index) => ({ value, index }))
    .filter(
      (pair): pair is { value: number; index: number } => pair.value !== null,
    );
  if (windPairs.length > 0) {
    const peakWind = windPairs.reduce((best, current) =>
      current.value > best.value ? current : best,
    );
    eventCandidates.push({
      type: "peak_wind",
      priority: 80,
      at: starts[peakWind.index],
      value: peakWind.value,
    });
  }

  for (let index = 1; index < temperature.length; index += 1) {
    const prev = temperature[index - 1];
    const curr = temperature[index];
    if (prev === null || curr === null) {
      continue;
    }
    const crossedFreezing =
      (prev > 32 && curr <= 32) || (prev < 32 && curr >= 32);
    if (crossedFreezing) {
      eventCandidates.push({
        type: "freezing_crossing",
        priority: 95,
        at: starts[index],
        from: prev,
        to: curr,
      });
      break;
    }
  }

  const severeRegex =
    /\b(thunderstorm|severe|tornado|hail|squall|blizzard|freezing rain|ice storm)\b/i;
  const shortForecast = (columns.shortForecast ?? []) as unknown[];
  for (let index = 0; index < shortForecast.length; index += 1) {
    const text = shortForecast[index];
    if (typeof text === "string" && severeRegex.test(text)) {
      eventCandidates.push({
        type: "severe_condition",
        priority: 100,
        at: starts[index],
        detail: text,
      });
    }
  }

  const topEvents = eventCandidates
    .sort((left, right) => {
      const leftPriority = Number(left.priority ?? 0);
      const rightPriority = Number(right.priority ?? 0);
      if (leftPriority !== rightPriority) {
        return rightPriority - leftPriority;
      }
      const leftAt = typeof left.at === "string" ? left.at : "";
      const rightAt = typeof right.at === "string" ? right.at : "";
      return leftAt.localeCompare(rightAt);
    })
    .slice(0, 6)
    .map(({ priority, ...event }) => event);

  const rainWindowsNextDay = buildWindows(
    nextDayIndices,
    starts,
    ends,
    precipitationChance,
    30,
    "rain_window",
  );
  const windyWindowsNextDay = buildWindows(
    nextDayIndices,
    starts,
    ends,
    windSpeed,
    15,
    "windy_window",
  );

  const dailyBuckets = new Map<
    string,
    {
      start: string | null;
      end: string | null;
      sum: number;
      count: number;
      high: number;
      low: number;
    }
  >();

  const referenceTodayMidnight =
    firstStart !== null
      ? Date.parse(`${firstStart.slice(0, 10)}T00:00:00${timeOffset}`)
      : Number.NaN;
  const todayMidnight = Number.isFinite(referenceTodayMidnight)
    ? referenceTodayMidnight
    : new Date(
        new Date().getFullYear(),
        new Date().getMonth(),
        new Date().getDate(),
        0,
        0,
        0,
        0,
      ).getTime();
  const tomorrowMidnight = todayMidnight + 24 * 60 * 60 * 1000;

  for (const index of weekIndices) {
    const ts = timestamps[index];
    const temp = temperature[index];
    const startStamp = starts[index];
    if (ts === null || temp === null || ts < todayMidnight || !startStamp) {
      continue;
    }
    const dayKey = startStamp.slice(0, 10);
    const existing = dailyBuckets.get(dayKey);
    if (existing) {
      existing.end = ends[index] ?? existing.end;
      existing.sum += temp;
      existing.count += 1;
      existing.high = Math.max(existing.high, temp);
      existing.low = Math.min(existing.low, temp);
      continue;
    }
    dailyBuckets.set(dayKey, {
      start: starts[index],
      end: ends[index],
      sum: temp,
      count: 1,
      high: temp,
      low: temp,
    });
  }

  const dailyAverages = [...dailyBuckets.entries()]
    .map(([day, bucket]) => ({
      day,
      start: bucket.start,
      end: bucket.end,
      avg: bucket.count > 0 ? bucket.sum / bucket.count : null,
      high: bucket.high,
      low: bucket.low,
    }))
    .sort((left, right) => left.day.localeCompare(right.day));

  const majorDailyTempShiftCandidates: Array<{
    end: string;
    summary: Record<string, unknown>;
  }> = [];
  for (let index = 1; index < dailyAverages.length; index += 1) {
    const previous = dailyAverages[index - 1];
    const current = dailyAverages[index];
    const previousDayTs = Date.parse(`${previous.day}T00:00:00${timeOffset}`);
    const currentDayTs = Date.parse(`${current.day}T00:00:00${timeOffset}`);

    if (
      !Number.isFinite(previousDayTs) ||
      !Number.isFinite(currentDayTs) ||
      previousDayTs < tomorrowMidnight ||
      currentDayTs < tomorrowMidnight ||
      currentDayTs - previousDayTs !== 24 * 60 * 60 * 1000 ||
      (currentDayKey !== null &&
        (previous.day <= currentDayKey || current.day <= currentDayKey))
    ) {
      continue;
    }

    const highDelta = Math.round((current.high - previous.high) * 10) / 10;
    const lowDelta = Math.round((current.low - previous.low) * 10) / 10;
    const highContributes = Math.abs(highDelta) >= 8;
    const lowContributes = Math.abs(lowDelta) >= 8;

    if (highContributes || lowContributes) {
      const contributor =
        highContributes && lowContributes
          ? "both"
          : highContributes
            ? "high"
            : "low";

      let direction = "mixed";
      if (contributor === "both") {
        if (highDelta > 0 && lowDelta > 0) {
          direction = "warming_both";
        } else if (highDelta < 0 && lowDelta < 0) {
          direction = "cooling_both";
        } else {
          direction = "mixed_both";
        }
      } else if (contributor === "high") {
        direction = highDelta > 0 ? "warming_high" : "cooling_high";
      } else {
        direction = lowDelta > 0 ? "warming_low" : "cooling_low";
      }

      const directionText =
        direction === "warming_both"
          ? "warming high and low"
          : direction === "cooling_both"
            ? "cooling high and low"
            : direction.replace(/_/g, " ");
      majorDailyTempShiftCandidates.push({
        end: `${current.day}T00:00:00${timeOffset}`,
        summary: {
          description: `${directionText} from ${previous.day} to ${current.day}`,
          highDelta,
          lowDelta,
        },
      });
    }
  }

  const majorDailyTempShifts = majorDailyTempShiftCandidates.map(
    (candidate) => candidate.summary,
  );

  const nextDayMajorShifts = majorDailyTempShiftCandidates
    .filter((candidate) => {
      const parsed = Date.parse(candidate.end);
      return (
        Number.isFinite(parsed) &&
        anchor !== undefined &&
        parsed <= anchor + 24 * 60 * 60 * 1000
      );
    })
    .map((candidate) => candidate.summary);

  const weekRainSeries = weekIndices
    .map((index) => precipitationChance[index])
    .filter((value): value is number => value !== null);
  const weekWindSeries = weekIndices
    .map((index) => windSpeed[index])
    .filter((value): value is number => value !== null);
  const weekTempSeries = weekIndices
    .map((index) => temperature[index])
    .filter((value): value is number => value !== null);

  const firstDayRainAvg =
    weekRainSeries.slice(0, 24).reduce((sum, value) => sum + value, 0) /
    Math.max(1, weekRainSeries.slice(0, 24).length);
  const lastDayRainAvg =
    weekRainSeries.slice(-24).reduce((sum, value) => sum + value, 0) /
    Math.max(1, weekRainSeries.slice(-24).length);

  const rainTrendDirection =
    lastDayRainAvg - firstDayRainAvg > 5
      ? "increasing"
      : lastDayRainAvg - firstDayRainAvg < -5
        ? "decreasing"
        : "steady";

  const dailyRainBuckets = new Map<
    string,
    {
      sum: number;
      count: number;
    }
  >();

  for (const index of weekIndices) {
    const rainChance = precipitationChance[index];
    const startStamp = starts[index];
    if (rainChance === null || !startStamp) {
      continue;
    }

    const dayKey = startStamp.slice(0, 10);
    const existing = dailyRainBuckets.get(dayKey);
    if (existing) {
      existing.sum += rainChance;
      existing.count += 1;
      continue;
    }

    dailyRainBuckets.set(dayKey, {
      sum: rainChance,
      count: 1,
    });
  }

  const dailyRainAverages = [...dailyRainBuckets.entries()]
    .map(([day, bucket]) => ({
      day,
      avg: bucket.count > 0 ? bucket.sum / bucket.count : 0,
    }))
    .sort((left, right) => left.day.localeCompare(right.day));

  const rainIncreaseStartDates: string[] = [];
  for (let index = 1; index < dailyRainAverages.length; index += 1) {
    const previous = dailyRainAverages[index - 1];
    const current = dailyRainAverages[index];
    const delta = current.avg - previous.avg;

    if (delta <= 5) {
      continue;
    }

    if (index === 1) {
      rainIncreaseStartDates.push(current.day);
      continue;
    }

    const priorDelta = previous.avg - dailyRainAverages[index - 2].avg;
    if (priorDelta <= 5) {
      rainIncreaseStartDates.push(current.day);
    }
  }

  const weekWindyWindows = buildWindows(
    weekIndices,
    starts,
    ends,
    windSpeed,
    15,
    "windy_window",
  ).slice(0, 5);

  return {
    format: "periods-derived-v1",
    rowCount,
    horizon: {
      start: starts.find((value) => value !== null) ?? null,
      end:
        [...ranges]
          .reverse()
          .find(
            (value): value is unknown[] =>
              Array.isArray(value) && typeof value[1] === "string",
          )?.[1] ?? null,
    },
    units: {
      temperature: columnMeta.temperature?.unitCode,
      dewpoint: columnMeta.dewpoint?.unitCode,
      windSpeed: columnMeta.windSpeed?.unitCode,
    },
    metrics: {
      temperature: summarizeNumericSeries(temperature),
      dewpoint: summarizeNumericSeries(dewpoint),
      windSpeed: summarizeNumericSeries(windSpeed),
      precipitationChance: summarizeNumericSeries(precipitationChance),
    },
    nextDayDetailed: {
      horizon: {
        start: nextDayIndices.length > 0 ? starts[nextDayIndices[0]] : null,
        end:
          nextDayIndices.length > 0
            ? ends[nextDayIndices[nextDayIndices.length - 1]]
            : null,
      },
      rainWindows: rainWindowsNextDay,
      windyWindows: windyWindowsNextDay,
      majorTemperatureShifts: nextDayMajorShifts.slice(0, 6),
      keyEvents: topEvents.filter((event) => {
        const at = event.at;
        if (typeof at !== "string") {
          return false;
        }
        const parsed = Date.parse(at);
        return (
          Number.isFinite(parsed) &&
          anchor !== undefined &&
          parsed <= anchor + 24 * 60 * 60 * 1000
        );
      }),
    },
    weekGeneralized: {
      horizon: {
        start: weekIndices.length > 0 ? starts[weekIndices[0]] : null,
        end:
          weekIndices.length > 0
            ? ends[weekIndices[weekIndices.length - 1]]
            : null,
      },
      temperature: {
        trend: summarizeNumericSeries(weekTempSeries).trend,
        dailyHighLow: dailyAverages.map((entry) => ({
          day: entry.day,
          high: entry.high,
          low: entry.low,
        })),
        majorTemperatureShifts: majorDailyTempShifts.slice(0, 6),
      },
      rainTrend: {
        direction: rainTrendDirection,
        firstDayAvg: Math.round(firstDayRainAvg * 10) / 10,
        lastDayAvg: Math.round(lastDayRainAvg * 10) / 10,
        peakChance:
          weekRainSeries.length > 0 ? Math.max(...weekRainSeries) : null,
        increaseStartDates: rainIncreaseStartDates,
      },
      windOverview: {
        max: weekWindSeries.length > 0 ? Math.max(...weekWindSeries) : null,
        windyTimeframes: weekWindyWindows,
      },
      notableEvents: topEvents,
    },
    topEvents,
  };
};

const compactPeriods = (
  periods: unknown,
  forecastType: ForecastType,
  maxPeriods?: number,
): Record<string, unknown> => {
  const isHourly = forecastType === "forecastHourly";

  const transformedPeriods =
    isHourly && Array.isArray(periods)
      ? periods.map((period) => {
          const row = { ...(period as Record<string, unknown>) };
          row.range = [row.startTime ?? null, row.endTime ?? null];
          delete row.startTime;
          delete row.endTime;
          return row;
        })
      : periods;

  const excludedFields = isHourly
    ? [
        "number",
        "name",
        "icon",
        "isDaytime",
        "relativeHumidity",
        "temperatureTrend",
        "detailedForecast",
      ]
    : [];

  const columnar = toColumnarRecords(
    transformedPeriods,
    maxPeriods,
    excludedFields,
  );

  if (!isHourly) {
    normalizeIconPrefix(columnar);
  }

  if (isHourly) {
    const fields = (columnar.fields ?? []) as string[];
    const columns = (columnar.columns ?? {}) as Record<string, unknown[]>;
    const columnMeta = (columnar.columnMeta ?? {}) as Record<
      string,
      Record<string, unknown>
    >;

    const temperatureUnits = columns.temperatureUnit;
    const hasTemperature = fields.includes("temperature");
    const hasTemperatureUnit = fields.includes("temperatureUnit");

    if (
      hasTemperature &&
      hasTemperatureUnit &&
      Array.isArray(temperatureUnits)
    ) {
      const normalizedUnits = temperatureUnits
        .filter((value) => value !== null && value !== undefined)
        .map((value) => String(value));

      const uniqueUnits = [...new Set(normalizedUnits)];
      if (uniqueUnits.length === 1) {
        const [unitCode] = uniqueUnits;
        columnMeta.temperature = {
          ...(columnMeta.temperature || {}),
          unitCode,
        };
        delete columns.temperatureUnit;
        columnar.fields = fields.filter((field) => field !== "temperatureUnit");
      }
    }

    normalizeWindSpeedUnit(columnar);
    normalizeDewpointRounding(columnar);
    columnar.columnMeta = columnMeta;

    return buildHourlyDerivedSummary(columnar);
  }

  return {
    format: "periods-columnar-v1",
    ...columnar,
  };
};

const compactGridData = (
  properties: Record<string, unknown>,
  maxPeriods?: number,
  compactGridFields?: string[],
): Record<string, unknown> => {
  const selectedFields =
    compactGridFields && compactGridFields.length > 0
      ? compactGridFields
      : Object.keys(properties).filter((key) => {
          const row = properties[key] as Record<string, unknown> | undefined;
          return !!row && Array.isArray(row.values);
        });

  const compact: Record<string, unknown> = {};

  for (const field of selectedFields) {
    const rawField = properties[field] as Record<string, unknown> | undefined;
    if (!rawField) {
      continue;
    }

    const { values: _values, ...metadata } = rawField;
    const values = toColumnarRecords(rawField.values, maxPeriods);

    compact[field] = {
      metadata,
      values,
    };
  }

  return compact;
};

const compactForecastRaw = (
  forecastType: ForecastType,
  forecastJson: ForecastJson,
  maxPeriods?: number,
  compactGridFields?: string[],
): Record<string, unknown> => {
  const properties = (forecastJson.properties ?? {}) as Record<string, unknown>;

  const compact: Record<string, unknown> = {
    encoding: "nws-forecast-compact-v1",
    forecastType,
    updated: properties.updated,
    generatedAt: properties.generatedAt,
  };

  if (forecastType === "forecast" || forecastType === "forecastHourly") {
    compact.periods = compactPeriods(
      properties.periods,
      forecastType,
      maxPeriods,
    );
  }

  if (forecastType === "forecastGridData") {
    compact.grid = compactGridData(properties, maxPeriods, compactGridFields);
  }

  return compact;
};

const extractHeadlineAndDescription = (
  forecastType: ForecastType,
  forecastJson: ForecastJson,
): { headline?: string; description?: string; effective?: Date } => {
  const properties = (forecastJson.properties ?? {}) as Record<string, unknown>;
  const updated = toIso(properties.updated as string | undefined);

  if (forecastType === "forecast" || forecastType === "forecastHourly") {
    const periods = properties.periods;
    if (Array.isArray(periods) && periods.length > 0) {
      const first = periods[0] as Record<string, unknown>;
      const name = first.name;
      const detailedForecast = first.detailedForecast;
      const shortForecast = first.shortForecast;
      return {
        headline: typeof name === "string" ? name : undefined,
        description:
          typeof detailedForecast === "string"
            ? detailedForecast
            : typeof shortForecast === "string"
              ? shortForecast
              : undefined,
        effective: updated,
      };
    }
  }

  const generatedAt = toIso(properties.generatedAt as string | undefined);
  return {
    headline:
      forecastType === "forecastGridData" ? "Forecast Grid Data" : undefined,
    description: "Latest NWS forecast payload",
    effective: updated ?? generatedAt,
  };
};

/**
 * Ingests NWS forecast data for a point location and emits a canonical alert-like payload.
 *
 * The function performs a two-step request flow:
 * 1. Calls `https://api.weather.gov/points/{lat},{lon}` to resolve forecast URLs.
 * 2. Fetches the selected forecast endpoint and normalizes metadata + raw payload.
 *
 * @param config - Forecast ingestion configuration, including coordinates and user agent.
 * @returns Canonical ingestion result with one synthesized forecast alert and optional raw payload.
 * @throws When `userAgent` is missing or either upstream request fails.
 */
export const ingest = async (
  config: ForecastIngestionConfig,
): Promise<IngestionResult<ForecastJson>> => {
  const logger = config.logger ?? defaultLogger;
  const fetchImpl = resolveFetch(config);
  const forecastType = config.forecastType ?? DEFAULT_FORECAST_TYPE;
  const rawMode = config.rawMode ?? DEFAULT_RAW_MODE;
  const includeRaw = config.includeRaw ?? true;
  const maxPeriods = normalizeMaxPeriods(config.maxPeriods);
  const userAgent = validateUserAgent(config.userAgent);

  const pointsUrl = `${POINTS_BASE_URL}/${config.lat},${config.lon}`;

  logger.info("nws-forecast:fetch-points", {
    pointsUrl,
    forecastType,
  });

  const pointsResponse = await fetchImpl(pointsUrl, {
    headers: {
      "User-Agent": userAgent,
    },
  });

  if (!pointsResponse.ok) {
    throw new Error(
      `nws-forecast: points fetch failed ${pointsResponse.status}`,
    );
  }

  const pointsJson = (await pointsResponse.json()) as PointsResponse;
  const forecastUrl = getForecastUrlFromPoints(pointsJson, forecastType);

  logger.info("nws-forecast:fetch-forecast", {
    forecastUrl,
    forecastType,
  });

  const forecastResponse = await fetchImpl(forecastUrl, {
    headers: {
      "User-Agent": userAgent,
    },
  });

  if (!forecastResponse.ok) {
    throw new Error(
      `nws-forecast: ${forecastType} fetch failed ${forecastResponse.status}`,
    );
  }

  const forecastJson = (await forecastResponse.json()) as ForecastJson;
  const compactRaw = compactForecastRaw(
    forecastType,
    forecastJson,
    maxPeriods,
    config.compactGridFields,
  );
  const extracted = extractHeadlineAndDescription(forecastType, forecastJson);

  const alert: Alert = {
    nwsId: `${forecastType}:${config.lat},${config.lon}:${forecastUrl}`,
    event: forecastType,
    headline: extracted.headline,
    description: extracted.description,
    shortDescription:
      buildShortDescriptionFromDescription(extracted.description, {
        preferPreText: true,
        maxChars: 4000,
      }) ?? extracted.headline,
    sent: extracted.effective,
    effective: extracted.effective,
    source: "nws-forecast",
    extra: {
      pointsUrl,
      forecastUrl,
      forecastType,
      location: {
        lat: config.lat,
        lon: config.lon,
      },
    },
  };

  return {
    alerts: [alert],
    raw: includeRaw
      ? ((rawMode === "compact" ? compactRaw : forecastJson) as ForecastJson)
      : undefined,
    meta: {
      pointsUrl,
      forecastUrl,
      forecastType,
      rawMode,
      maxPeriods: maxPeriods ?? null,
      compactPreview: compactRaw,
    },
    dedupeKeys: [hashDedupeKey(alert.nwsId)],
  };
};
