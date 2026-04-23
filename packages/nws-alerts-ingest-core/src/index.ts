import { createHash } from "crypto";
import type { Geometry } from "geojson";

/**
 * Canonical normalized alert shape produced by ingestion adapters.
 */
export type Alert = {
  /** Stable provider-specific alert identifier (for example, NWS CAP ID). */
  nwsId: string;
  /** Alert event type (for example, Tornado Warning). */
  event?: string;
  /** Human-readable title or short summary. */
  headline?: string;
  /** Full alert narrative body. */
  description?: string;
  /** Condensed narrative intended for short-context retrieval/search. */
  shortDescription?: string;
  /** GeoJSON geometry associated with the alert, when available. */
  geometry?: Geometry | null;
  /** Time the alert message was sent by the source provider. */
  sent?: Date;
  /** Time the alert became effective. */
  effective?: Date;
  /** Forecast onset/start time, if provided. */
  onset?: Date;
  /** Expiration time of alert validity. */
  expires?: Date;
  /** End time of the event, if distinct from expiration. */
  ends?: Date;
  /** Originating data source name or identifier. */
  source: string;
  /** Additional provider-specific fields not mapped to canonical fields. */
  extra?: Record<string, unknown>;
};

/**
 * Logger contract used by ingestion modules.
 */
export type IngestionLogger = {
  /** Logs informational events. */
  info: (message: string, meta?: unknown) => void;
  /** Logs non-fatal warnings. */
  warn: (message: string, meta?: unknown) => void;
  /** Logs errors. */
  error: (message: string, meta?: unknown) => void;
};

/**
 * Base runtime configuration for ingestion modules.
 */
export type IngestionConfig = {
  /** Latitude used for location-aware filtering or lookups. */
  lat: number;
  /** Longitude used for location-aware filtering or lookups. */
  lon: number;
  /** Optional HTTP User-Agent value for upstream API requests. */
  userAgent?: string;
  /** Optional fetch implementation for environments without global fetch. */
  fetch?: typeof fetch;
  /** Optional clock injection for deterministic tests. */
  now?: () => Date;
  /** Optional logger implementation used during ingestion operations. */
  logger?: IngestionLogger;
};

/**
 * Generic ingestion output payload.
 *
 * @typeParam T - Raw upstream response payload type.
 */
export type IngestionResult<T = unknown> = {
  /** Canonical alerts produced by the ingestion module. */
  alerts: Alert[];
  /** Optional untouched upstream payload for debugging or replay. */
  raw?: T;
  /** Optional implementation-specific metadata about ingestion execution. */
  meta?: Record<string, unknown>;
  /** Stable content-based keys for downstream deduplication workflows. */
  dedupeKeys: string[];
};

/**
 * No-op logger implementation used as a safe default.
 */
export const defaultLogger: IngestionLogger = {
  info: (_message: string, _meta?: unknown) => undefined,
  warn: (_message: string, _meta?: unknown) => undefined,
  error: (_message: string, _meta?: unknown) => undefined,
};

/**
 * Resolves a fetch implementation from config or global runtime.
 *
 * @param config - Ingestion runtime configuration.
 * @returns Fetch function to use for HTTP requests.
 * @throws When no fetch implementation is available.
 */
export const resolveFetch = (config: IngestionConfig): typeof fetch => {
  if (config.fetch) {
    return config.fetch;
  }
  if (typeof fetch === "function") {
    return fetch;
  }
  throw new Error("No fetch implementation available. Provide config.fetch.");
};

/**
 * Resolves current time using an injected clock when provided.
 *
 * @param config - Ingestion runtime configuration.
 * @returns The current timestamp.
 */
export const resolveNow = (config: IngestionConfig): Date => {
  return config.now ? config.now() : new Date();
};

/**
 * Generates a deterministic SHA-256 hash used as a deduplication key.
 *
 * @param input - Input string to hash.
 * @returns Lowercase hexadecimal SHA-256 digest.
 */
export const hashDedupeKey = (input: string): string => {
  return createHash("sha256").update(input).digest("hex");
};

/**
 * Determines whether a point lies inside a polygon using ray casting.
 *
 * @param point - Coordinate tuple in `[longitude, latitude]` order.
 * @param polygon - Closed polygon ring coordinates.
 * @returns `true` when the point is inside the polygon.
 */
export const pointInPolygon = (
  point: [number, number],
  polygon: number[][],
) => {
  let inside = false;
  for (let i = 0, j = polygon.length - 1; i < polygon.length; j = i++) {
    const xi = polygon[i][0];
    const yi = polygon[i][1];
    const xj = polygon[j][0];
    const yj = polygon[j][1];

    const intersect =
      yi > point[1] !== yj > point[1] &&
      point[0] < ((xj - xi) * (point[1] - yi)) / (yj - yi) + xi;
    if (intersect) inside = !inside;
  }
  return inside;
};

/**
 * Converts optional date-like values to a valid `Date` instance.
 *
 * @param value - String, Date, null, or undefined date input.
 * @returns Parsed Date when valid; otherwise `undefined`.
 */
export const toIso = (value?: string | Date | null): Date | undefined => {
  if (!value) {
    return undefined;
  }
  if (value instanceof Date) {
    return value;
  }
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return undefined;
  }
  return parsed;
};

export type ShortDescriptionOptions = {
  preferPreText?: boolean;
  removeLinePatterns?: RegExp[];
  stripLatLonCoordinates?: boolean;
  maxChars?: number;
};

export const extractPreText = (description?: string): string | undefined => {
  if (!description) {
    return undefined;
  }

  const matches = Array.from(description.matchAll(/<pre>([\s\S]*?)<\/pre>/gi));
  if (!matches.length) {
    return undefined;
  }

  return matches
    .map((match) => match[1]?.trim() ?? "")
    .filter((chunk) => chunk.length > 0)
    .join("\n")
    .trim();
};

const decodeHtmlEntities = (value: string): string => {
  return value
    .replace(/&nbsp;/gi, " ")
    .replace(/&amp;/gi, "&")
    .replace(/&lt;/gi, "<")
    .replace(/&gt;/gi, ">")
    .replace(/&quot;/gi, '"')
    .replace(/&#39;/gi, "'");
};

const stripHtmlToPlainText = (value: string): string => {
  return decodeHtmlEntities(
    value
      .replace(/<style[\s\S]*?<\/style>/gi, " ")
      .replace(/<script[\s\S]*?<\/script>/gi, " ")
      .replace(/<head[\s\S]*?<\/head>/gi, " ")
      .replace(/<br\s*\/?\s*>/gi, "\n")
      .replace(/<\/p>/gi, "\n")
      .replace(/<\/div>/gi, "\n")
      .replace(/<\/li>/gi, "\n")
      .replace(/<\/h[1-6]>/gi, "\n")
      .replace(/<[^>]+>/g, " "),
  );
};

const normalizeShortDescriptionText = (value: string): string => {
  return value
    .replace(/[\t\r]+/g, " ")
    .replace(/[ ]{2,}/g, " ")
    .replace(/\n{3,}/g, "\n\n")
    .trim();
};

const trimToMaxChars = (value: string, maxChars: number): string => {
  if (value.length <= maxChars) {
    return value;
  }

  const trimmed = value.slice(0, maxChars);
  const lastSpace = trimmed.lastIndexOf(" ");
  if (lastSpace > maxChars * 0.7) {
    return trimmed.slice(0, lastSpace).trim();
  }

  return trimmed.trim();
};

export const buildShortDescriptionFromDescription = (
  description?: string,
  options: ShortDescriptionOptions = {},
): string | undefined => {
  if (!description || !description.trim()) {
    return undefined;
  }

  const maxChars = options.maxChars ?? 4000;
  const sourceText =
    options.preferPreText === true
      ? (extractPreText(description) ?? description)
      : description;

  let workingText = sourceText.includes("<")
    ? stripHtmlToPlainText(sourceText)
    : sourceText;

  if (options.stripLatLonCoordinates) {
    workingText = workingText
      .replace(/\bLAT\.\.\.LON\b[^\n]*/gi, " ")
      .replace(/\bLAT\.\.\.LON\b/gi, " ");
  }

  let lines = workingText
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => line.length > 0);

  if (options.removeLinePatterns?.length) {
    lines = lines.filter(
      (line) =>
        !options.removeLinePatterns?.some((pattern) => pattern.test(line)),
    );
  }

  const joined = lines.join("\n");
  const normalized = normalizeShortDescriptionText(joined);
  if (!normalized) {
    return undefined;
  }

  return trimToMaxChars(normalized, maxChars);
};
