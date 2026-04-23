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
 * Subset of NWS active alerts GeoJSON feature structure consumed by this adapter.
 */
export type NwsActiveFeature = {
  /** Stable alert identifier provided by NWS. */
  id: string;
  properties: {
    /** Alert event name (for example, "Tornado Warning"). */
    event?: string;
    /** Time the alert was sent. */
    sent?: string;
    /** Time the alert becomes effective. */
    effective?: string;
    /** Time the alert begins (if provided). */
    onset?: string;
    /** Time the alert expires. */
    expires?: string;
    /** Time the alert ends (if provided). */
    ends?: string;
    /** Short human-readable summary. */
    headline?: string;
    /** Full alert text body. */
    description?: string;
  };
  /** GeoJSON geometry payload for the alert area when available. */
  geometry?: unknown;
};

/**
 * Minimal NWS active alerts response shape.
 */
export type NwsActiveResponse = {
  /** Collection of alert features returned by NWS. */
  features?: NwsActiveFeature[];
};

/**
 * Fetches and normalizes active NWS alerts for a given point location.
 *
 * @param config - Runtime ingestion configuration.
 * @returns Canonical alert output with raw payload and ingest metadata.
 * @throws When the upstream NWS request fails.
 */
export const ingest = async (
  config: IngestionConfig,
): Promise<IngestionResult<NwsActiveResponse>> => {
  const logger = config.logger ?? defaultLogger;
  const fetchImpl = resolveFetch(config);
  const url = `https://api.weather.gov/alerts/active?point=${config.lat},${config.lon}`;

  logger.info("nws-active:fetch", { url });
  const response = await fetchImpl(url, {
    headers: {
      "User-Agent": config.userAgent ?? "nws-alerts-ingest-nws-active",
    },
  });

  if (!response.ok) {
    throw new Error(`nws-active: fetch failed ${response.status}`);
  }

  const data = (await response.json()) as NwsActiveResponse;
  const features = data.features ?? [];

  const alerts: Alert[] = features.map((feature) => ({
    nwsId: feature.id,
    event: feature.properties?.event,
    geometry: (feature.geometry as never) ?? null,
    sent: toIso(feature.properties?.sent),
    effective: toIso(feature.properties?.effective),
    onset: toIso(feature.properties?.onset),
    expires: toIso(feature.properties?.expires),
    ends: toIso(feature.properties?.ends),
    headline: feature.properties?.headline,
    description: feature.properties?.description,
    shortDescription:
      buildShortDescriptionFromDescription(feature.properties?.description, {
        preferPreText: true,
        maxChars: 4000,
      }) ?? feature.properties?.headline,
    source: "nws-active",
    extra: {
      sourceUrl: url,
    },
  }));

  return {
    alerts,
    raw: data,
    meta: {
      sourceUrl: url,
      featureCount: features.length,
    },
    dedupeKeys: alerts.map((alert) => hashDedupeKey(alert.nwsId)),
  };
};
