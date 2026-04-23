import { XMLParser } from "fast-xml-parser";
import moment from "moment-timezone";
import type { Polygon } from "geojson";
import {
  Alert,
  buildShortDescriptionFromDescription,
  IngestionConfig,
  IngestionResult,
  defaultLogger,
  pointInPolygon,
  resolveFetch,
} from "nws-alerts-ingest-core";

const MD_URL = "https://www.spc.noaa.gov/products/spcmdrss.xml";

type RssItem = {
  title?: string;
  guid?: string;
  description?: string;
  pubDate?: string;
};

const parseRssItems = (rss: string): RssItem[] => {
  const parser = new XMLParser({
    ignoreAttributes: false,
  });
  const parsed = parser.parse(rss);
  const items = parsed?.rss?.channel?.item ?? [];
  if (Array.isArray(items)) {
    return items;
  }
  return [items];
};

const extractLatLonFromMdDescription = (desc: string): number[][] => {
  const rawLatLonArray = desc
    .substring(desc.indexOf("LAT...LON") + 12, desc.lastIndexOf("</pre>"))
    .replace(/\r/g, "")
    .replace(/\t/g, "")
    .replace(/\n/g, " ")
    .trim()
    .split(" ");

  const splitLatLonArray = rawLatLonArray
    .filter((latlon) => latlon.length === 8)
    .map((latlon) => {
      const nwsLat = latlon.substring(0, 4);
      const nwsLon = latlon.substring(4, 8);
      return [convertNwsLatToFullLat(nwsLat), convertNwsLonToFullLon(nwsLon)];
    });

  return splitLatLonArray;
};

const convertNwsLatToFullLat = (nwsLat: string) => {
  return Number(`${nwsLat.substring(0, 2)}.${nwsLat.substring(2, 4)}`);
};

const convertNwsLonToFullLon = (nwsLon: string) => {
  const periodAddedLon = `${nwsLon.substring(0, 2)}.${nwsLon.substring(2, 4)}`;
  return Number(nwsLon.substring(0, 1)) < 5
    ? Number(`-1${periodAddedLon}`)
    : Number(`-${periodAddedLon}`);
};

/**
 * Ingests SPC Mesoscale Discussion RSS entries and filters them by source location.
 *
 * @param config - Runtime ingestion configuration with location and optional transport overrides.
 * @returns Canonical alerts, filtered raw RSS items, metadata, and dedupe keys.
 * @throws When the RSS feed request fails.
 */
export const ingest = async (
  config: IngestionConfig,
): Promise<IngestionResult<{ items: RssItem[] }>> => {
  const logger = config.logger ?? defaultLogger;
  const fetchImpl = resolveFetch(config);
  const sourceLocation = { lat: config.lat, lon: config.lon };

  logger.info("spc-md:fetch", { url: MD_URL });
  const response = await fetchImpl(MD_URL, {
    headers: {
      "User-Agent": config.userAgent ?? "nws-alerts-ingest-spc-md",
    },
  });

  if (!response.ok) {
    throw new Error(`spc-md: fetch failed ${response.status}`);
  }

  const text = await response.text();
  const items = parseRssItems(text);

  const filteredItems: RssItem[] = [];
  const alerts: Alert[] = items
    .filter((item) => item.description?.includes("LAT...LON"))
    .map((item) => {
      const coordinates = extractLatLonFromMdDescription(
        item.description ?? "",
      );
      const geometry: Polygon = {
        type: "Polygon",
        coordinates: [coordinates.map(([lat, lon]) => [lon, lat])],
      };
      return {
        item,
        nwsId: item.guid ?? item.title ?? "spc-md",
        headline: item.title ?? "SPC Mesoscale Discussion",
        event: "SPC Mesoscale Discussion",
        description: item.description,
        shortDescription:
          buildShortDescriptionFromDescription(item.description, {
            preferPreText: true,
            stripLatLonCoordinates: true,
            maxChars: 4000,
          }) ??
          item.title ??
          "SPC Mesoscale Discussion",
        effective: moment.tz(item.pubDate, "America/Chicago").toDate(),
        onset: moment.tz(item.pubDate, "America/Chicago").toDate(),
        sent: moment.tz(item.pubDate, "America/Chicago").toDate(),
        geometry,
        source: "spc-md",
        extra: {
          sourceUrl: MD_URL,
          location: sourceLocation,
        },
      };
    })
    .filter((entry) => {
      const alert = entry as Alert & { item: RssItem };
      const geometry = alert.geometry as Polygon;
      if (!geometry?.coordinates?.[0]?.length) {
        return false;
      }
      const latLonPolygon = geometry.coordinates[0].map(([lon, lat]) => [
        lat,
        lon,
      ]);
      const isRelevant = pointInPolygon(
        [sourceLocation.lat, sourceLocation.lon],
        latLonPolygon,
      );
      if (!isRelevant) {
        logger.info("spc-md:outside-polygon", { nwsId: alert.nwsId });
      } else {
        filteredItems.push(alert.item);
      }
      return isRelevant;
    })
    .map((entry) => {
      const { item: _item, ...alert } = entry as Alert & { item: RssItem };
      return alert;
    });

  return {
    alerts,
    raw: { items: filteredItems },
    meta: {
      sourceUrl: MD_URL,
      itemCount: filteredItems.length,
      sourceLocation,
    },
    dedupeKeys: alerts.map((alert) => alert.nwsId),
  };
};
