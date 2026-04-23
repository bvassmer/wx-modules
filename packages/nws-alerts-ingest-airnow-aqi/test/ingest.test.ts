import { describe, expect, it } from "vitest";
import { ingest } from "../src/index";

const observedResponse = [
  {
    DateObserved: "2026-04-17",
    HourObserved: 18,
    LocalTimeZone: "CST",
    ReportingArea: "Tulsa",
    StateCode: "OK",
    Latitude: 36.154,
    Longitude: -95.992,
    ParameterName: "PM2.5",
    AQI: 112,
    Category: {
      Number: 3,
      Name: "Unhealthy for Sensitive Groups",
    },
  },
  {
    DateObserved: "2026-04-17",
    HourObserved: 18,
    LocalTimeZone: "CST",
    ReportingArea: "Tulsa",
    StateCode: "OK",
    Latitude: 36.154,
    Longitude: -95.992,
    ParameterName: "OZONE",
    AQI: 86,
    Category: {
      Number: 2,
      Name: "Moderate",
    },
  },
  {
    DateObserved: "2026-04-17",
    HourObserved: 18,
    LocalTimeZone: "CST",
    ReportingArea: "FarAway",
    StateCode: "TX",
    Latitude: 34,
    Longitude: -100,
    ParameterName: "PM2.5",
    AQI: 190,
    Category: {
      Number: 4,
      Name: "Unhealthy",
    },
  },
];

const forecastResponse = [
  {
    DateIssue: "2026-04-17T12:00:00-05:00",
    DateForecast: "2026-04-17",
    ReportingArea: "Tulsa",
    StateCode: "OK",
    Latitude: 36.154,
    Longitude: -95.992,
    ParameterName: "PM2.5",
    AQI: 118,
    Category: {
      Number: 3,
      Name: "Unhealthy for Sensitive Groups",
    },
    ActionDay: true,
    Discussion: "Wildfire smoke may affect sensitive groups.",
  },
  {
    DateIssue: "2026-04-17T12:00:00-05:00",
    DateForecast: "2026-04-17",
    ReportingArea: "Tulsa",
    StateCode: "OK",
    Latitude: 36.154,
    Longitude: -95.992,
    ParameterName: "OZONE",
    AQI: 92,
    Category: {
      Number: 2,
      Name: "Moderate",
    },
    ActionDay: false,
    Discussion: "Moderate ozone conditions expected.",
  },
  {
    DateIssue: "2026-04-17T12:00:00-05:00",
    DateForecast: "2026-04-18",
    ReportingArea: "Tulsa",
    StateCode: "OK",
    Latitude: 36.154,
    Longitude: -95.992,
    ParameterName: "PM2.5",
    AQI: 138,
    Category: {
      Number: 3,
      Name: "Unhealthy for Sensitive Groups",
    },
    ActionDay: true,
    Discussion: "Smoke persists into tomorrow.",
  },
];

const fetchImpl = async (input: string | URL) => {
  const url = String(input);
  if (url.includes("/observation/latLong/current/")) {
    return {
      ok: true,
      json: async () => observedResponse,
    } as Response;
  }

  if (url.includes("/forecast/latLong/")) {
    return {
      ok: true,
      json: async () => forecastResponse,
    } as Response;
  }

  throw new Error(`Unexpected URL ${url}`);
};

describe("airnow-aqi ingest", () => {
  it("returns nearest-area observed and forecast alerts", async () => {
    const result = await ingest({
      lat: 36.15,
      lon: -95.99,
      apiKey: "test-key",
      fetch: fetchImpl as typeof fetch,
      forecastLookaheadDays: 2,
      now: () => new Date("2026-04-17T18:30:00.000Z"),
    });

    expect(result.alerts).toHaveLength(3);
    expect(result.alerts[0].event).toBe("AirNow AQI Threshold Alert");
    expect(result.alerts[0].headline).toContain("AQI 112");
    expect((result.alerts[0].extra as any)?.dominantPollutant).toBe("PM2.5");
    expect(result.alerts[1].event).toBe("AirNow AQI Forecast Alert");
    expect(result.alerts[2].event).toBe("AirNow AQI Forecast Alert");
    expect((result.meta as any)?.observedCount).toBe(3);
    expect((result.meta as any)?.forecastCount).toBeGreaterThanOrEqual(3);
    expect(result.dedupeKeys).toHaveLength(3);
  });
});