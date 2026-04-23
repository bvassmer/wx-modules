import moment from "moment-timezone";
import type { Feature, FeatureCollection } from "geojson";
import { booleanPointInPolygon, point } from "@turf/turf";
import {
  Alert,
  IngestionConfig,
  IngestionResult,
  defaultLogger,
  resolveFetch,
  resolveNow,
} from "nws-alerts-ingest-core";

const WPC_PRODUCT = {
  DAY_1: "day1",
  DAY_2: "day2",
  DAY_3: "day3",
  DAY_4: "day4",
  DAY_5: "day5",
};

type WpcProduct = keyof typeof WPC_PRODUCT | string;

const WPC_EXC_RAINFALL_URLS: Record<
  string,
  { GEOJSON: string; TSA_PNG: string }
> = {
  [WPC_PRODUCT.DAY_1]: {
    GEOJSON:
      "https://www.wpc.ncep.noaa.gov/exper/eromap/geojson/Day1_Latest.geojson",
    TSA_PNG: "https://www.wpc.ncep.noaa.gov/exper/eromap/cwamaps/TSA_Day1.png",
  },
  [WPC_PRODUCT.DAY_2]: {
    GEOJSON:
      "https://www.wpc.ncep.noaa.gov/exper/eromap/geojson/Day2_Latest.geojson",
    TSA_PNG: "https://www.wpc.ncep.noaa.gov/exper/eromap/cwamaps/TSA_Day2.png",
  },
  [WPC_PRODUCT.DAY_3]: {
    GEOJSON:
      "https://www.wpc.ncep.noaa.gov/exper/eromap/geojson/Day3_Latest.geojson",
    TSA_PNG: "https://www.wpc.ncep.noaa.gov/exper/eromap/cwamaps/TSA_Day3.png",
  },
  [WPC_PRODUCT.DAY_4]: {
    GEOJSON:
      "https://www.wpc.ncep.noaa.gov/exper/eromap/geojson/Day4_Latest.geojson",
    TSA_PNG: "https://www.wpc.ncep.noaa.gov/exper/eromap/cwamaps/TSA_Day4.png",
  },
  [WPC_PRODUCT.DAY_5]: {
    GEOJSON:
      "https://www.wpc.ncep.noaa.gov/exper/eromap/geojson/Day5_Latest.geojson",
    TSA_PNG: "https://www.wpc.ncep.noaa.gov/exper/eromap/cwamaps/TSA_Day5.png",
  },
};

type SearchWindow = {
  timeStr: string;
  time: moment.Moment;
  start: moment.Moment;
  end: moment.Moment;
  products: string[];
};

const generateOutlookSearchTimes = (
  referenceNow: moment.Moment,
): SearchWindow[] => {
  const base = referenceNow.clone();
  return [
    {
      timeStr: "0100",
      time: base.clone().hour(1).minute(0).second(0).millisecond(0),
      start: base.clone().hour(0).minute(45).second(0).millisecond(0),
      end: base.clone().hour(2).minute(0).second(0).millisecond(0),
      products: [WPC_PRODUCT.DAY_1],
    },
    {
      timeStr: "0830",
      time: base.clone().hour(8).minute(30).second(0).millisecond(0),
      start: base.clone().hour(8).minute(15).second(0).millisecond(0),
      end: base.clone().hour(9).minute(30).second(0).millisecond(0),
      products: [
        WPC_PRODUCT.DAY_1,
        WPC_PRODUCT.DAY_2,
        WPC_PRODUCT.DAY_3,
        WPC_PRODUCT.DAY_4,
        WPC_PRODUCT.DAY_5,
      ],
    },
    {
      timeStr: "1600",
      time: base.clone().hour(16).minute(0).second(0).millisecond(0),
      start: base.clone().hour(15).minute(45).second(0).millisecond(0),
      end: base.clone().hour(17).minute(30).second(0).millisecond(0),
      products: [WPC_PRODUCT.DAY_1],
    },
    {
      timeStr: "2030",
      time: base.clone().hour(20).minute(30).second(0).millisecond(0),
      start: base.clone().hour(19).minute(15).second(0).millisecond(0),
      end: base.clone().hour(21).minute(30).second(0).millisecond(0),
      products: [
        WPC_PRODUCT.DAY_2,
        WPC_PRODUCT.DAY_3,
        WPC_PRODUCT.DAY_4,
        WPC_PRODUCT.DAY_5,
      ],
    },
  ];
};

const selectProductsForNow = (now: moment.Moment): string[] => {
  const windows = generateOutlookSearchTimes(now);
  const window = windows.find((item) => now.isBetween(item.start, item.end));
  return window ? window.products : [];
};

const buildErrDescription = (product: string, labels: string[]): string => {
  const labelText = labels.length
    ? `Risk labels: ${labels.join(", ")}.`
    : "Risk labels unavailable.";
  return `WPC Excessive Rainfall Outlook ${product.toUpperCase()}. ${labelText}`;
};

const buildErrShortDescription = (
  description: string,
  labels: string[],
): string => {
  const normalizedDescription = description.replace(/\s+/g, " ").trim();
  const summary = labels.length
    ? `Key risk labels: ${labels.join(", ")}.`
    : "No risk labels were provided in the matched features.";
  return `${normalizedDescription} ${summary}`.trim();
};

/**
 * Ingests WPC Excessive Rainfall outlook products relevant to the current issuance window.
 *
 * @param config - Runtime ingestion configuration with location and optional overrides.
 * @returns Canonical alerts with matched products, raw payload summary, and metadata.
 */
export const ingest = async (
  config: IngestionConfig,
): Promise<IngestionResult<Record<string, unknown>>> => {
  const logger = config.logger ?? defaultLogger;
  const fetchImpl = resolveFetch(config);
  const now = moment(resolveNow(config)).tz("America/Chicago").utc();
  const sourceLocation = { lat: config.lat, lon: config.lon };
  const products = selectProductsForNow(now);

  if (!products.length) {
    return {
      alerts: [],
      raw: { products: [] },
      meta: { issuedAt: now.toISOString(), reason: "outside-window" },
      dedupeKeys: [],
    };
  }

  const alerts: Alert[] = [];
  const matchedProducts: Array<{
    product: string;
    featureCount: number;
    labels: string[];
    sourceUrl: string;
    mapUrl: string;
  }> = [];

  for (const product of products) {
    const urls = WPC_EXC_RAINFALL_URLS[product];
    if (!urls) continue;

    logger.info("wpc-excessive-rainfall:fetch", { product, url: urls.GEOJSON });
    const response = await fetchImpl(urls.GEOJSON, {
      headers: {
        "User-Agent":
          config.userAgent ?? "nws-alerts-ingest-wpc-excessive-rainfall",
      },
    });

    if (!response.ok) {
      logger.warn("wpc-excessive-rainfall:fetch-failed", {
        product,
        status: response.status,
      });
      continue;
    }

    const data = (await response.json()) as FeatureCollection;
    const myPoint = point([sourceLocation.lon, sourceLocation.lat]);
    const matching = data.features?.filter((feature: Feature) => {
      try {
        return booleanPointInPolygon(myPoint, feature as any);
      } catch {
        return false;
      }
    });

    if (matching && matching.length > 0) {
      const labels = matching
        .map((feature) => {
          const properties = feature.properties as Record<string, unknown>;
          const label = properties?.LABEL;
          return typeof label === "string" ? label : null;
        })
        .filter((label): label is string => Boolean(label));

      const description = buildErrDescription(product, labels);
      alerts.push({
        nwsId: urls.GEOJSON,
        event: "WPC Excessive Rainfall",
        headline: `WPC Excessive Rainfall Risk - ${product.toUpperCase()}`,
        description,
        shortDescription: buildErrShortDescription(description, labels),
        sent: new Date(now.toISOString()),
        source: "wpc-excessive-rainfall",
        extra: {
          product,
          sourceUrl: urls.GEOJSON,
          mapUrl: urls.TSA_PNG,
          featureCount: matching.length,
          labels,
          location: sourceLocation,
        },
      });

      matchedProducts.push({
        product,
        featureCount: matching.length,
        labels,
        sourceUrl: urls.GEOJSON,
        mapUrl: urls.TSA_PNG,
      });
    }
  }

  return {
    alerts,
    raw: { products: matchedProducts },
    meta: {
      issuedAt: now.toISOString(),
      requestedProducts: products,
      matchedProductCount: matchedProducts.length,
      sourceLocation,
    },
    dedupeKeys: alerts.map((alert) => alert.nwsId),
  };
};
