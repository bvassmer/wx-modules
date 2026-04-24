import moment from "moment-timezone";
import type { Feature, FeatureCollection, Position } from "geojson";
import {
  booleanPointInPolygon,
  featureCollection,
  featureEach,
  point,
  polygon,
} from "@turf/turf";
// @ts-ignore - jsdom does not have type definitions in this project
import { JSDOM } from "jsdom";
// @ts-ignore - jquery does not have type definitions in this project
import jquery from "jquery";
import {
  Alert,
  buildShortDescriptionFromDescription,
  IngestionConfig,
  IngestionResult,
  defaultLogger,
  resolveFetch,
  resolveNow,
} from "nws-alerts-ingest-core";

const SPC_CONV_DAY_12_OUTLOOK_TYPE = {
  CATEGORICAL: "cat",
  TORNADO: "torn",
  SIGNIFICANT_TORNADO: "sigtorn",
  HAIL: "hail",
  SIGNIFICANT_HAIL: "sighail",
  WIND: "wind",
  SIGNIFICANT_WIND: "sigwind",
  HTML: "html",
};
const SPC_CONV_DAY_3_OUTLOOK_TYPE = {
  CATEGORICAL: "cat",
  PROB: "prob",
  SIGNIFICANT_PROB: "sigprob",
  HTML: "html",
};

const SPC_RISKS = {
  TWO_PERCENT: "0.02",
  FIVE_PERCENT: "0.05",
  TEN_PERCENT: "0.10",
  FIFTEEN_PERCENT: "0.15",
  THIRTY_PERCENT: "0.30",
  FOURTY_FIVE_PERCENT: "0.45",
  SIXTY_PERCENT: "0.60",
  SIGN: "SIGN",
  CIG1: "CIG1",
  CIG2: "CIG2",
  CIG3: "CIG3",
  THUNDERSTORM: "TSTM",
  MARGINAL: "MRGL",
  SLIGHT: "SLGT",
  ENHANCED: "ENH",
  MODERATE: "MDT",
  HIGH: "HIGH",
};

type SPC_CONV_OUTLOOK_DAY_12 = {
  cat?: string;
  torn?: string;
  hail?: string;
  wind?: string;
  sigtorn?: string;
  sighail?: string;
  sigwind?: string;
  html?: string;
};

type SPC_CONV_OUTLOOK_DAY_3 = {
  cat?: string;
  prob?: string;
  sigprob?: string;
  html?: string;
};

type SPC_CONV_OUTLOOK_DAY_45678 = {
  prob?: string;
  html?: string;
};

type SPC_CONV_OUTLOOK_DAY_12_FEATURES = {
  cat?: FeatureCollection;
  torn?: FeatureCollection;
  hail?: FeatureCollection;
  wind?: FeatureCollection;
  sigtorn?: FeatureCollection;
  sighail?: FeatureCollection;
  sigwind?: FeatureCollection;
  html?: string;
};

type SPC_CONV_OUTLOOK_DAY_3_FEATURES = {
  cat?: FeatureCollection;
  prob?: FeatureCollection;
  sigprob?: FeatureCollection;
  html?: string;
};

type SPC_CONV_OUTLOOK_DAY_45678_FEATURES = {
  prob?: FeatureCollection;
  html?: string;
};

/**
 * Builds local search windows (in UTC) used to determine which SPC issuance time
 * should be fetched for a given run.
 */
const generateOutlookSearchTimes = (referenceNow: moment.Moment) => {
  const base = referenceNow.clone();
  return {
    "0100": {
      START: base.clone().hour(0).minute(0).second(0).millisecond(0),
      END: base.clone().hour(2).minute(0).second(0).millisecond(0),
    },
    "0600": {
      START: base.clone().hour(5).minute(0).second(0).millisecond(0),
      END: base.clone().hour(7).minute(0).second(0).millisecond(0),
    },
    "0730": {
      START: base.clone().hour(6).minute(0).second(0).millisecond(0),
      END: base.clone().hour(8).minute(0).second(0).millisecond(0),
    },
    "0900": {
      START: base.clone().hour(8).minute(0).second(0).millisecond(0),
      END: base.clone().hour(10).minute(0).second(0).millisecond(0),
    },
    "1200": {
      START: base.clone().hour(11).minute(0).second(0).millisecond(0),
      END: base.clone().hour(13).minute(0).second(0).millisecond(0),
    },
    "1300": {
      START: base.clone().hour(12).minute(0).second(0).millisecond(0),
      END: base.clone().hour(14).minute(0).second(0).millisecond(0),
    },
    "1630": {
      START: base.clone().hour(15).minute(0).second(0).millisecond(0),
      END: base.clone().hour(17).minute(0).second(0).millisecond(0),
    },
    "1730": {
      START: base.clone().hour(16).minute(0).second(0).millisecond(0),
      END: base.clone().hour(18).minute(0).second(0).millisecond(0),
    },
    "1930": {
      START: base.clone().hour(19).minute(0).second(0).millisecond(0),
      END: base.clone().hour(21).minute(0).second(0).millisecond(0),
    },
    "2000": {
      START: base.clone().hour(19).minute(0).second(0).millisecond(0),
      END: base.clone().hour(21).minute(0).second(0).millisecond(0),
    },
  };
};

const normalizeSpcGeoJsonType = ({
  day,
  type,
}: {
  day: number;
  type: string;
}) => {
  if (day <= 2) {
    switch (type) {
      case SPC_CONV_DAY_12_OUTLOOK_TYPE.SIGNIFICANT_TORNADO:
        return "cigtorn";
      case SPC_CONV_DAY_12_OUTLOOK_TYPE.SIGNIFICANT_HAIL:
        return "cighail";
      case SPC_CONV_DAY_12_OUTLOOK_TYPE.SIGNIFICANT_WIND:
        return "cigwind";
      default:
        return type;
    }
  }

  return type;
};

const getSpcConvDay123GeoJsonUrl = ({
  day,
  time,
  type,
  now,
}: {
  day: number;
  time: string;
  type: string;
  now: moment.Moment;
}) => {
  const geoJsonType = normalizeSpcGeoJsonType({ day, type });
  if (time === "0100") {
    return `https://www.spc.noaa.gov/products/outlook/archive/${now
      .clone()
      .add(1, "day")
      .format("YYYY")}/day${day}otlk_${moment()
      .tz("America/Chicago")
      .utc()
      .year(now.year())
      .month(now.month())
      .date(now.date())
      .add(1, "day")
      .format("YYYYMMDD")}_${time}_${geoJsonType}.lyr.geojson`;
  }
  return `https://www.spc.noaa.gov/products/outlook/archive/${now
    .clone()
    .format("YYYY")}/day${day}otlk_${moment()
    .tz("America/Chicago")
    .utc()
    .year(now.year())
    .month(now.month())
    .date(now.date())
    .format("YYYYMMDD")}_${time}_${geoJsonType}.lyr.geojson`;
};

const getSpcConvDay45678GeoJsonUrl = ({
  day,
  now,
}: {
  day: number;
  now: moment.Moment;
}) => {
  return `https://www.spc.noaa.gov/products/exper/day4-8/archive/${now
    .clone()
    .format("YYYY")}/day${day}prob_${moment()
    .tz("America/Chicago")
    .utc()
    .year(now.year())
    .month(now.month())
    .date(now.date())
    .format("YYYYMMDD")}.lyr.geojson`;
};

const getSpcConvDay123HtmlUrl = ({
  day,
  time,
  now,
}: {
  day: number;
  time: string;
  now: moment.Moment;
}) => {
  if (time === "0100") {
    return `https://www.spc.noaa.gov/products/outlook/archive/${now
      .clone()
      .add(1, "day")
      .format("YYYY")}/day${day}otlk_${moment()
      .tz("America/Chicago")
      .utc()
      .year(now.year())
      .month(now.month())
      .date(now.date())
      .add(1, "day")
      .format("YYYYMMDD")}_${time}.html`;
  }
  return `https://www.spc.noaa.gov/products/outlook/archive/${now
    .clone()
    .format("YYYY")}/day${day}otlk_${moment()
    .tz("America/Chicago")
    .utc()
    .year(now.year())
    .month(now.month())
    .date(now.date())
    .format("YYYYMMDD")}_${time}.html`;
};

const getSpcConvDay45678HtmlUrl = (now: moment.Moment) => {
  return `https://www.spc.noaa.gov/products/exper/day4-8/archive/${now
    .clone()
    .format("YYYY")}/day4-8_${moment()
    .tz("America/Chicago")
    .utc()
    .year(now.year())
    .month(now.month())
    .date(now.date())
    .format("YYYYMMDD")}.html`;
};

/**
 * Resolves Day 1 convective outlook URLs for the currently active issuance window.
 *
 * @param now Current timestamp in UTC/Chicago-normalized context.
 * @returns URL set for Day 1 products, or `undefined` when no issuance window matches.
 */
const getConvOutlookDay1Urls = (
  now: moment.Moment,
): SPC_CONV_OUTLOOK_DAY_12 | undefined => {
  const outputSearchTimes = generateOutlookSearchTimes(now);
  let spcDay1Time: string | undefined;

  if (
    now.isBetween(
      outputSearchTimes["1200"].START,
      outputSearchTimes["1200"].END,
    )
  ) {
    spcDay1Time = "1200";
  }
  if (
    now.isBetween(
      outputSearchTimes["1300"].START,
      outputSearchTimes["1300"].END,
    )
  ) {
    spcDay1Time = "1300";
  }
  if (
    now.isBetween(
      outputSearchTimes["1630"].START,
      outputSearchTimes["1630"].END,
    )
  ) {
    spcDay1Time = "1630";
  }
  if (
    now.isBetween(
      outputSearchTimes["2000"].START,
      outputSearchTimes["2000"].END,
    )
  ) {
    spcDay1Time = "2000";
  }
  if (
    now.isBetween(
      outputSearchTimes["0100"].START,
      outputSearchTimes["0100"].END,
    )
  ) {
    spcDay1Time = "0100";
  }

  if (!spcDay1Time) {
    return undefined;
  }

  const urls: SPC_CONV_OUTLOOK_DAY_12 = {};
  Object.values(SPC_CONV_DAY_12_OUTLOOK_TYPE).forEach((type: string) => {
    urls[type as keyof SPC_CONV_OUTLOOK_DAY_12] = getSpcConvDay123GeoJsonUrl({
      day: 1,
      time: spcDay1Time,
      type,
      now,
    });
  });
  urls.html = getSpcConvDay123HtmlUrl({ day: 1, time: spcDay1Time, now });
  return urls;
};

/**
 * Resolves Day 2 convective outlook URLs for the currently active issuance window.
 *
 * @param now Current timestamp in UTC/Chicago-normalized context.
 * @returns URL set for Day 2 products, or `undefined` when no issuance window matches.
 */
const getConvOutlookDay2Urls = (
  now: moment.Moment,
): SPC_CONV_OUTLOOK_DAY_12 | undefined => {
  const outputSearchTimes = generateOutlookSearchTimes(now);
  let spcDay2Time: string | undefined;
  if (
    now.isBetween(
      outputSearchTimes["0600"].START,
      outputSearchTimes["0600"].END,
    )
  ) {
    spcDay2Time = "0600";
  }
  if (
    now.isBetween(
      outputSearchTimes["1730"].START,
      outputSearchTimes["1730"].END,
    )
  ) {
    spcDay2Time = "1730";
  }
  if (!spcDay2Time) {
    return undefined;
  }

  const urls: SPC_CONV_OUTLOOK_DAY_12 = {};
  Object.values(SPC_CONV_DAY_12_OUTLOOK_TYPE).forEach((type: string) => {
    urls[type as keyof SPC_CONV_OUTLOOK_DAY_12] = getSpcConvDay123GeoJsonUrl({
      day: 2,
      time: spcDay2Time,
      type,
      now,
    });
  });
  urls.html = getSpcConvDay123HtmlUrl({ day: 2, time: spcDay2Time, now });
  return urls;
};

/**
 * Resolves Day 3 convective outlook URLs for the currently active issuance window.
 *
 * @param now Current timestamp in UTC/Chicago-normalized context.
 * @returns URL set for Day 3 products, or `undefined` when no issuance window matches.
 */
const getConvOutlookDay3Urls = (
  now: moment.Moment,
): SPC_CONV_OUTLOOK_DAY_3 | undefined => {
  const outputSearchTimes = generateOutlookSearchTimes(now);
  let spcDay3Time: string | undefined;
  if (
    now.isBetween(
      outputSearchTimes["0730"].START,
      outputSearchTimes["0730"].END,
    )
  ) {
    spcDay3Time = "0730";
  }
  if (
    now.isBetween(
      outputSearchTimes["1930"].START,
      outputSearchTimes["1930"].END,
    )
  ) {
    spcDay3Time = "1930";
  }
  if (!spcDay3Time) {
    return undefined;
  }

  const urls: SPC_CONV_OUTLOOK_DAY_3 = {};
  Object.values(SPC_CONV_DAY_3_OUTLOOK_TYPE).forEach((type: string) => {
    urls[type as keyof SPC_CONV_OUTLOOK_DAY_3] = getSpcConvDay123GeoJsonUrl({
      day: 3,
      time: spcDay3Time,
      type,
      now,
    });
  });
  urls.html = getSpcConvDay123HtmlUrl({ day: 3, time: spcDay3Time, now });
  return urls;
};

/**
 * Resolves Day 4-8 convective outlook URLs for the currently active issuance window.
 *
 * @returns URL set for the requested day, or `undefined` when no issuance window matches.
 */
const getConvOutlookDay45678Urls = ({
  day,
  now,
}: {
  day: number;
  now: moment.Moment;
}): SPC_CONV_OUTLOOK_DAY_45678 | undefined => {
  const outputSearchTimes = generateOutlookSearchTimes(now);
  let spcDayTime: string | undefined;
  if (
    now.isBetween(
      outputSearchTimes["0900"].START,
      outputSearchTimes["0900"].END,
    )
  ) {
    spcDayTime = "0900";
  }
  if (!spcDayTime) {
    return undefined;
  }

  return {
    prob: getSpcConvDay45678GeoJsonUrl({ day, now }),
    html: getSpcConvDay45678HtmlUrl(now),
  };
};

/**
 * Fetches GeoJSON from a URL, returning `undefined` when the URL is absent or the
 * response is non-2xx.
 */
const getUrlJson = async (fetchImpl: typeof fetch, url?: string) => {
  if (!url) return undefined;
  const response = await fetchImpl(url);
  if (!response.ok) return undefined;
  return (await response.json()) as FeatureCollection;
};

/**
 * Fetches text/HTML from a URL, returning `undefined` when the URL is absent or the
 * response is non-2xx.
 */
const getUrlText = async (fetchImpl: typeof fetch, url?: string) => {
  if (!url) return undefined;
  const response = await fetchImpl(url);
  if (!response.ok) return undefined;
  return response.text();
};

/**
 * Converts relative SPC references in Day 1-3 HTML payloads into absolute URLs so
 * rendered content keeps working outside the SPC site context.
 */
const fixRelativeLinksInSpcHtml = (html?: string) => {
  const year = moment().tz("America/Chicago").year();
  return html
    ?.replace(/href="\//gm, `href="https://www.spc.noaa.gov/`)
    .replace(/src="\//gm, `src="https://www.spc.noaa.gov/`)
    .replace(
      /src="day/gm,
      `src="https://www.spc.noaa.gov/products/outlook/archive/${year}/day`,
    );
};

/**
 * Converts relative SPC references in Day 4-8 HTML payloads into absolute URLs so
 * rendered content keeps working outside the SPC site context.
 */
const fixRelativeLinksInSpcDay45678Html = (html?: string) => {
  const year = moment().tz("America/Chicago").year();
  return html
    ?.replace(/href="\//gm, `href="https://www.spc.noaa.gov/`)
    .replace(/src="\//gm, `src="https://www.spc.noaa.gov/`)
    .replace(
      /src="day/gm,
      `src="https://www.spc.noaa.gov/products/exper/day4-8/archive/${year}/day`,
    );
};

/**
 * Extracts the nested day 4-8 probability table from the SPC HTML document.
 */
const extractDay45678Table = (html?: string) => {
  if (!html) return undefined;
  const dom = new JSDOM(html);
  const window = dom.window;
  const $ = jquery(window as any);
  const nestedTable = $(
    "body > table:nth-child(5) > tbody > tr > td:nth-child(2) > table:nth-child(4)",
  );
  return nestedTable.prop("outerHTML") as string | undefined;
};

/**
 * Filters a feature collection to polygons containing the configured point.
 *
 * @returns A feature collection containing only matching features.
 */
const filterFeatureCollectionByLocation = ({
  fc,
  lat,
  lon,
}: {
  fc?: FeatureCollection;
  lat: number;
  lon: number;
}): FeatureCollection | undefined => {
  if (!fc?.features?.length) {
    return fc;
  }

  const myPoint = point([lon, lat]);
  const features = fc.features.filter((feature) => {
    try {
      return booleanPointInPolygon(myPoint, feature as any);
    } catch {
      return false;
    }
  });

  return featureCollection(features as Feature[]);
};

/**
 * Finds the highest categorical convective risk level for a location.
 *
 * Risk order: `HIGH > MDT > ENH > SLGT > MRGL > TSTM`.
 */
const findHighestCatRisk = ({
  fc,
  lat,
  lon,
}: {
  fc: FeatureCollection;
  lat: number;
  lon: number;
}) => {
  const found = {
    [SPC_RISKS.THUNDERSTORM]: false,
    [SPC_RISKS.MARGINAL]: false,
    [SPC_RISKS.SLIGHT]: false,
    [SPC_RISKS.ENHANCED]: false,
    [SPC_RISKS.MODERATE]: false,
    [SPC_RISKS.HIGH]: false,
  };
  const myPoint = point([lon, lat]);
  featureEach(fc, (feature) => {
    const feat = feature as Feature & {
      geometry: { coordinates: Position[][][] };
    };
    if (feat?.geometry?.coordinates?.length) {
      const poly = polygon(feat.geometry.coordinates as Position[][]);
      if (poly && booleanPointInPolygon(myPoint, poly)) {
        found[feat.properties?.LABEL] = true;
      }
    }
  });

  if (found[SPC_RISKS.HIGH]) return SPC_RISKS.HIGH;
  if (found[SPC_RISKS.MODERATE]) return SPC_RISKS.MODERATE;
  if (found[SPC_RISKS.ENHANCED]) return SPC_RISKS.ENHANCED;
  if (found[SPC_RISKS.SLIGHT]) return SPC_RISKS.SLIGHT;
  if (found[SPC_RISKS.MARGINAL]) return SPC_RISKS.MARGINAL;
  if (found[SPC_RISKS.THUNDERSTORM]) return SPC_RISKS.THUNDERSTORM;
  return null;
};

/**
 * Finds the highest tornado probability risk level for a location.
 */
const findHighestTornRisk = ({
  fc,
  lat,
  lon,
}: {
  fc: FeatureCollection;
  lat: number;
  lon: number;
}) => {
  const found = {
    [SPC_RISKS.TWO_PERCENT]: false,
    [SPC_RISKS.FIVE_PERCENT]: false,
    [SPC_RISKS.TEN_PERCENT]: false,
    [SPC_RISKS.FIFTEEN_PERCENT]: false,
    [SPC_RISKS.THIRTY_PERCENT]: false,
    [SPC_RISKS.FOURTY_FIVE_PERCENT]: false,
    [SPC_RISKS.SIXTY_PERCENT]: false,
  };
  const myPoint = point([lon, lat]);
  featureEach(fc, (feature) => {
    const feat = feature as Feature & {
      geometry: { coordinates: Position[][][] };
    };
    if (feat?.geometry?.coordinates?.length) {
      const poly = polygon(feat.geometry.coordinates as Position[][]);
      if (poly && booleanPointInPolygon(myPoint, poly)) {
        found[feat.properties?.LABEL] = true;
      }
    }
  });

  if (found[SPC_RISKS.SIXTY_PERCENT]) return SPC_RISKS.SIXTY_PERCENT;
  if (found[SPC_RISKS.FOURTY_FIVE_PERCENT])
    return SPC_RISKS.FOURTY_FIVE_PERCENT;
  if (found[SPC_RISKS.THIRTY_PERCENT]) return SPC_RISKS.THIRTY_PERCENT;
  if (found[SPC_RISKS.FIFTEEN_PERCENT]) return SPC_RISKS.FIFTEEN_PERCENT;
  if (found[SPC_RISKS.TEN_PERCENT]) return SPC_RISKS.TEN_PERCENT;
  if (found[SPC_RISKS.FIVE_PERCENT]) return SPC_RISKS.FIVE_PERCENT;
  if (found[SPC_RISKS.TWO_PERCENT]) return SPC_RISKS.TWO_PERCENT;
  return null;
};

/**
 * Finds the highest hail/wind probability risk level for a location.
 */
const findHighestHailWindRisk = ({
  fc,
  lat,
  lon,
}: {
  fc: FeatureCollection;
  lat: number;
  lon: number;
}) => {
  const found = {
    [SPC_RISKS.FIVE_PERCENT]: false,
    [SPC_RISKS.FIFTEEN_PERCENT]: false,
    [SPC_RISKS.THIRTY_PERCENT]: false,
    [SPC_RISKS.FOURTY_FIVE_PERCENT]: false,
    [SPC_RISKS.SIXTY_PERCENT]: false,
  };
  const myPoint = point([lon, lat]);
  featureEach(fc, (feature) => {
    const feat = feature as Feature & {
      geometry: { coordinates: Position[][][] };
    };
    if (feat?.geometry?.coordinates?.length) {
      const poly = polygon(feat.geometry.coordinates as Position[][]);
      if (poly && booleanPointInPolygon(myPoint, poly)) {
        found[feat.properties?.LABEL] = true;
      }
    }
  });

  if (found[SPC_RISKS.SIXTY_PERCENT]) return SPC_RISKS.SIXTY_PERCENT;
  if (found[SPC_RISKS.FOURTY_FIVE_PERCENT])
    return SPC_RISKS.FOURTY_FIVE_PERCENT;
  if (found[SPC_RISKS.THIRTY_PERCENT]) return SPC_RISKS.THIRTY_PERCENT;
  if (found[SPC_RISKS.FIFTEEN_PERCENT]) return SPC_RISKS.FIFTEEN_PERCENT;
  if (found[SPC_RISKS.FIVE_PERCENT]) return SPC_RISKS.FIVE_PERCENT;
  return null;
};

/**
 * Detects the highest local SPC conditional intensity group for a location.
 */
const findHighestCigRisk = ({
  fcs,
  lat,
  lon,
}: {
  fcs: Array<FeatureCollection | undefined>;
  lat: number;
  lon: number;
}) => {
  const found = {
    [SPC_RISKS.CIG1]: false,
    [SPC_RISKS.CIG2]: false,
    [SPC_RISKS.CIG3]: false,
    [SPC_RISKS.SIGN]: false,
  };
  const myPoint = point([lon, lat]);
  fcs.forEach((fc) => {
    featureEach(fc ?? featureCollection([]), (feature) => {
      const feat = feature as Feature & {
        geometry: { coordinates: Position[][][] };
      };
      if (feat?.geometry?.coordinates?.length) {
        const poly = polygon(feat.geometry.coordinates as Position[][]);
        if (poly && booleanPointInPolygon(myPoint, poly)) {
          const label = feat.properties?.LABEL;
          if (label in found) {
            found[label] = true;
          }
        }
      }
    });
  });

  if (found[SPC_RISKS.CIG3]) return SPC_RISKS.CIG3;
  if (found[SPC_RISKS.CIG2]) return SPC_RISKS.CIG2;
  if (found[SPC_RISKS.CIG1]) return SPC_RISKS.CIG1;
  if (found[SPC_RISKS.SIGN]) return SPC_RISKS.SIGN;
  return null;
};

const getSpcCigStars = (risk: string | null | undefined) => {
  switch (risk) {
    case SPC_RISKS.CIG3:
      return "***";
    case SPC_RISKS.CIG2:
      return "**";
    case SPC_RISKS.CIG1:
    case SPC_RISKS.SIGN:
      return "*";
    default:
      return "";
  }
};

const buildSpcDay12Headline = ({
  day,
  levels,
}: {
  day: 1 | 2;
  levels:
    | SPC_CONV_OUTLOOK_DAY_12
    | {
        cat?: string | null;
        torn?: string | null;
        hail?: string | null;
        wind?: string | null;
        sigtorn?: string | null;
        sighail?: string | null;
        sigwind?: string | null;
      }
    | undefined;
}) => {
  return `SPC Conv Day ${day} - ${levels?.cat ?? "N/A"} T${
    levels?.torn ?? ""
  }${getSpcCigStars(levels?.sigtorn)} H${levels?.hail ?? ""}${getSpcCigStars(
    levels?.sighail,
  )} W${levels?.wind ?? ""}${getSpcCigStars(levels?.sigwind)}`;
};

const buildSpcDay3Headline = ({
  levels,
}: {
  levels:
    | SPC_CONV_OUTLOOK_DAY_3
    | {
        cat?: string | null;
        prob?: string | null;
        sigprob?: string | null;
      }
    | undefined;
}) => {
  return `SPC Conv Day 3 - ${levels?.cat ?? "N/A"} P${levels?.prob ?? ""}${getSpcCigStars(
    levels?.sigprob,
  )}`;
};

/**
 * Ingests SPC convective outlook products (Days 1-8) for a single source location.
 *
 * The ingestor:
 * - resolves currently valid SPC issuance windows,
 * - downloads geojson/html products for each available day,
 * - filters geometries by the configured latitude/longitude,
 * - computes highest applicable risk labels, and
 * - returns normalized alert objects plus raw payloads.
 *
 * @param config Ingestion configuration with `lat`, `lon`, optional `fetch`, `now`, and `logger`.
 * @returns Ingestion result containing generated alerts, raw day payloads, metadata, and dedupe keys.
 */
export const ingest = async (
  config: IngestionConfig,
): Promise<IngestionResult<Record<string, unknown>>> => {
  const logger = config.logger ?? defaultLogger;
  const fetchImpl = resolveFetch(config);
  const now = moment(resolveNow(config)).tz("America/Chicago").utc();
  const sourceLocation = { lat: config.lat, lon: config.lon };

  const day1Urls = getConvOutlookDay1Urls(now);
  const day2Urls = getConvOutlookDay2Urls(now);
  const day3Urls = getConvOutlookDay3Urls(now);
  const day4Urls = getConvOutlookDay45678Urls({ day: 4, now });
  const day5Urls = getConvOutlookDay45678Urls({ day: 5, now });
  const day6Urls = getConvOutlookDay45678Urls({ day: 6, now });
  const day7Urls = getConvOutlookDay45678Urls({ day: 7, now });
  const day8Urls = getConvOutlookDay45678Urls({ day: 8, now });

  const day1 = day1Urls
    ? {
        cat: filterFeatureCollectionByLocation({
          fc: await getUrlJson(fetchImpl, day1Urls.cat),
          lat: sourceLocation.lat,
          lon: sourceLocation.lon,
        }),
        hail: filterFeatureCollectionByLocation({
          fc: await getUrlJson(fetchImpl, day1Urls.hail),
          lat: sourceLocation.lat,
          lon: sourceLocation.lon,
        }),
        sighail: filterFeatureCollectionByLocation({
          fc: await getUrlJson(fetchImpl, day1Urls.sighail),
          lat: sourceLocation.lat,
          lon: sourceLocation.lon,
        }),
        wind: filterFeatureCollectionByLocation({
          fc: await getUrlJson(fetchImpl, day1Urls.wind),
          lat: sourceLocation.lat,
          lon: sourceLocation.lon,
        }),
        sigwind: filterFeatureCollectionByLocation({
          fc: await getUrlJson(fetchImpl, day1Urls.sigwind),
          lat: sourceLocation.lat,
          lon: sourceLocation.lon,
        }),
        torn: filterFeatureCollectionByLocation({
          fc: await getUrlJson(fetchImpl, day1Urls.torn),
          lat: sourceLocation.lat,
          lon: sourceLocation.lon,
        }),
        sigtorn: filterFeatureCollectionByLocation({
          fc: await getUrlJson(fetchImpl, day1Urls.sigtorn),
          lat: sourceLocation.lat,
          lon: sourceLocation.lon,
        }),
        html: fixRelativeLinksInSpcHtml(
          await getUrlText(fetchImpl, day1Urls.html),
        ),
      }
    : undefined;

  const day2 = day2Urls
    ? {
        cat: filterFeatureCollectionByLocation({
          fc: await getUrlJson(fetchImpl, day2Urls.cat),
          lat: sourceLocation.lat,
          lon: sourceLocation.lon,
        }),
        hail: filterFeatureCollectionByLocation({
          fc: await getUrlJson(fetchImpl, day2Urls.hail),
          lat: sourceLocation.lat,
          lon: sourceLocation.lon,
        }),
        sighail: filterFeatureCollectionByLocation({
          fc: await getUrlJson(fetchImpl, day2Urls.sighail),
          lat: sourceLocation.lat,
          lon: sourceLocation.lon,
        }),
        wind: filterFeatureCollectionByLocation({
          fc: await getUrlJson(fetchImpl, day2Urls.wind),
          lat: sourceLocation.lat,
          lon: sourceLocation.lon,
        }),
        sigwind: filterFeatureCollectionByLocation({
          fc: await getUrlJson(fetchImpl, day2Urls.sigwind),
          lat: sourceLocation.lat,
          lon: sourceLocation.lon,
        }),
        torn: filterFeatureCollectionByLocation({
          fc: await getUrlJson(fetchImpl, day2Urls.torn),
          lat: sourceLocation.lat,
          lon: sourceLocation.lon,
        }),
        sigtorn: filterFeatureCollectionByLocation({
          fc: await getUrlJson(fetchImpl, day2Urls.sigtorn),
          lat: sourceLocation.lat,
          lon: sourceLocation.lon,
        }),
        html: fixRelativeLinksInSpcHtml(
          await getUrlText(fetchImpl, day2Urls.html),
        ),
      }
    : undefined;

  const day3 = day3Urls
    ? {
        cat: filterFeatureCollectionByLocation({
          fc: await getUrlJson(fetchImpl, day3Urls.cat),
          lat: sourceLocation.lat,
          lon: sourceLocation.lon,
        }),
        prob: filterFeatureCollectionByLocation({
          fc: await getUrlJson(fetchImpl, day3Urls.prob),
          lat: sourceLocation.lat,
          lon: sourceLocation.lon,
        }),
        sigprob: filterFeatureCollectionByLocation({
          fc: await getUrlJson(fetchImpl, day3Urls.sigprob),
          lat: sourceLocation.lat,
          lon: sourceLocation.lon,
        }),
        html: fixRelativeLinksInSpcHtml(
          await getUrlText(fetchImpl, day3Urls.html),
        ),
      }
    : undefined;

  const day4 = day4Urls
    ? {
        prob: filterFeatureCollectionByLocation({
          fc: await getUrlJson(fetchImpl, day4Urls.prob),
          lat: sourceLocation.lat,
          lon: sourceLocation.lon,
        }),
        html: fixRelativeLinksInSpcDay45678Html(
          extractDay45678Table(await getUrlText(fetchImpl, day4Urls.html)),
        ),
      }
    : undefined;
  const day5 = day5Urls
    ? {
        prob: filterFeatureCollectionByLocation({
          fc: await getUrlJson(fetchImpl, day5Urls.prob),
          lat: sourceLocation.lat,
          lon: sourceLocation.lon,
        }),
        html: fixRelativeLinksInSpcDay45678Html(
          extractDay45678Table(await getUrlText(fetchImpl, day5Urls.html)),
        ),
      }
    : undefined;
  const day6 = day6Urls
    ? {
        prob: filterFeatureCollectionByLocation({
          fc: await getUrlJson(fetchImpl, day6Urls.prob),
          lat: sourceLocation.lat,
          lon: sourceLocation.lon,
        }),
        html: fixRelativeLinksInSpcDay45678Html(
          extractDay45678Table(await getUrlText(fetchImpl, day6Urls.html)),
        ),
      }
    : undefined;
  const day7 = day7Urls
    ? {
        prob: filterFeatureCollectionByLocation({
          fc: await getUrlJson(fetchImpl, day7Urls.prob),
          lat: sourceLocation.lat,
          lon: sourceLocation.lon,
        }),
        html: fixRelativeLinksInSpcDay45678Html(
          extractDay45678Table(await getUrlText(fetchImpl, day7Urls.html)),
        ),
      }
    : undefined;
  const day8 = day8Urls
    ? {
        prob: filterFeatureCollectionByLocation({
          fc: await getUrlJson(fetchImpl, day8Urls.prob),
          lat: sourceLocation.lat,
          lon: sourceLocation.lon,
        }),
        html: fixRelativeLinksInSpcDay45678Html(
          extractDay45678Table(await getUrlText(fetchImpl, day8Urls.html)),
        ),
      }
    : undefined;

  const alerts: Alert[] = [];

  const day1Levels = day1
    ? {
        cat: findHighestCatRisk({
          fc: day1.cat ?? featureCollection([]),
          lat: sourceLocation.lat,
          lon: sourceLocation.lon,
        }),
        torn: findHighestTornRisk({
          fc: day1.torn ?? featureCollection([]),
          lat: sourceLocation.lat,
          lon: sourceLocation.lon,
        }),
        hail: findHighestHailWindRisk({
          fc: day1.hail ?? featureCollection([]),
          lat: sourceLocation.lat,
          lon: sourceLocation.lon,
        }),
        wind: findHighestHailWindRisk({
          fc: day1.wind ?? featureCollection([]),
          lat: sourceLocation.lat,
          lon: sourceLocation.lon,
        }),
        sigtorn: findHighestCigRisk({
          fcs: [day1.sigtorn, day1.torn],
          lat: sourceLocation.lat,
          lon: sourceLocation.lon,
        }),
        sighail: findHighestCigRisk({
          fcs: [day1.sighail, day1.hail],
          lat: sourceLocation.lat,
          lon: sourceLocation.lon,
        }),
        sigwind: findHighestCigRisk({
          fcs: [day1.sigwind, day1.wind],
          lat: sourceLocation.lat,
          lon: sourceLocation.lon,
        }),
      }
    : undefined;

  if (day1?.html && day1Levels?.cat) {
    alerts.push({
      nwsId: day1Urls?.html ?? "spc-day1",
      event: "SPC Convective Outlook Day 1",
      headline: buildSpcDay12Headline({ day: 1, levels: day1Levels }),
      description: day1.html,
      shortDescription:
        buildShortDescriptionFromDescription(day1.html, {
          preferPreText: true,
          maxChars: 4000,
        }) ?? `SPC Conv Day 1 - ${day1Levels.cat}`,
      sent: new Date(now.toISOString()),
      source: "spc-convective-outlook",
      extra: {
        day: 1,
        levels: day1Levels,
        urls: day1Urls,
        location: sourceLocation,
      },
    });
  }

  const day2Levels = day2
    ? {
        cat: findHighestCatRisk({
          fc: day2.cat ?? featureCollection([]),
          lat: sourceLocation.lat,
          lon: sourceLocation.lon,
        }),
        torn: findHighestTornRisk({
          fc: day2.torn ?? featureCollection([]),
          lat: sourceLocation.lat,
          lon: sourceLocation.lon,
        }),
        hail: findHighestHailWindRisk({
          fc: day2.hail ?? featureCollection([]),
          lat: sourceLocation.lat,
          lon: sourceLocation.lon,
        }),
        wind: findHighestHailWindRisk({
          fc: day2.wind ?? featureCollection([]),
          lat: sourceLocation.lat,
          lon: sourceLocation.lon,
        }),
        sigtorn: findHighestCigRisk({
          fcs: [day2.sigtorn, day2.torn],
          lat: sourceLocation.lat,
          lon: sourceLocation.lon,
        }),
        sighail: findHighestCigRisk({
          fcs: [day2.sighail, day2.hail],
          lat: sourceLocation.lat,
          lon: sourceLocation.lon,
        }),
        sigwind: findHighestCigRisk({
          fcs: [day2.sigwind, day2.wind],
          lat: sourceLocation.lat,
          lon: sourceLocation.lon,
        }),
      }
    : undefined;

  if (day2?.html && day2Levels?.cat) {
    alerts.push({
      nwsId: day2Urls?.html ?? "spc-day2",
      event: "SPC Convective Outlook Day 2",
      headline: buildSpcDay12Headline({ day: 2, levels: day2Levels }),
      description: day2.html,
      shortDescription:
        buildShortDescriptionFromDescription(day2.html, {
          preferPreText: true,
          maxChars: 4000,
        }) ?? `SPC Conv Day 2 - ${day2Levels.cat}`,
      sent: new Date(now.toISOString()),
      source: "spc-convective-outlook",
      extra: {
        day: 2,
        levels: day2Levels,
        urls: day2Urls,
        location: sourceLocation,
      },
    });
  }

  const day3Levels = day3
    ? {
        cat: findHighestCatRisk({
          fc: day3.cat ?? featureCollection([]),
          lat: sourceLocation.lat,
          lon: sourceLocation.lon,
        }),
        prob: findHighestHailWindRisk({
          fc: day3.prob ?? featureCollection([]),
          lat: sourceLocation.lat,
          lon: sourceLocation.lon,
        }),
        sigprob: findHighestCigRisk({
          fcs: [day3.sigprob, day3.prob],
          lat: sourceLocation.lat,
          lon: sourceLocation.lon,
        }),
      }
    : undefined;

  if (day3?.html && day3Levels?.cat) {
    alerts.push({
      nwsId: day3Urls?.html ?? "spc-day3",
      event: "SPC Convective Outlook Day 3",
      headline: buildSpcDay3Headline({ levels: day3Levels }),
      description: day3.html,
      shortDescription:
        buildShortDescriptionFromDescription(day3.html, {
          preferPreText: true,
          maxChars: 4000,
        }) ?? `SPC Conv Day 3 - ${day3Levels.cat}`,
      sent: new Date(now.toISOString()),
      source: "spc-convective-outlook",
      extra: {
        day: 3,
        levels: day3Levels,
        urls: day3Urls,
        location: sourceLocation,
      },
    });
  }

  const day45678Levels = async (
    dayNum: number,
    data?: SPC_CONV_OUTLOOK_DAY_45678_FEATURES,
    urls?: SPC_CONV_OUTLOOK_DAY_45678,
  ) => {
    if (!data?.html) return;
    const prob = findHighestHailWindRisk({
      fc: data.prob ?? featureCollection([]),
      lat: sourceLocation.lat,
      lon: sourceLocation.lon,
    });
    if (!prob) return;
    alerts.push({
      nwsId: urls?.html ?? `spc-day${dayNum}`,
      event: `SPC Convective Outlook Day ${dayNum}`,
      headline: `SPC Conv Day ${dayNum} - P${prob}`,
      description: data.html,
      shortDescription:
        buildShortDescriptionFromDescription(data.html, {
          preferPreText: true,
          maxChars: 4000,
        }) ?? `SPC Conv Day ${dayNum} - P${prob}`,
      sent: new Date(now.toISOString()),
      source: "spc-convective-outlook",
      extra: {
        day: dayNum,
        levels: { prob },
        urls,
        location: sourceLocation,
      },
    });
  };

  await day45678Levels(4, day4, day4Urls);
  await day45678Levels(5, day5, day5Urls);
  await day45678Levels(6, day6, day6Urls);
  await day45678Levels(7, day7, day7Urls);
  await day45678Levels(8, day8, day8Urls);

  logger.info("spc-convective-outlook:alerts", { count: alerts.length });

  return {
    alerts,
    raw: {
      day1,
      day2,
      day3,
      day4,
      day5,
      day6,
      day7,
      day8,
    },
    meta: {
      issuedAt: now.toISOString(),
      sourceLocation,
    },
    dedupeKeys: alerts.map((alert) => alert.nwsId),
  };
};
