import { describe, expect, it, vi, afterEach } from "vitest";
import { ingest } from "../src/index";

type FetchCall = {
  url: string;
  userAgent?: string;
};

const pointsFixture = {
  properties: {
    forecast: "https://api.weather.gov/gridpoints/OUN/56,95/forecast",
    forecastHourly:
      "https://api.weather.gov/gridpoints/OUN/56,95/forecast/hourly",
    forecastGridData: "https://api.weather.gov/gridpoints/OUN/56,95",
  },
};

const forecastFixture = {
  properties: {
    updated: "2026-02-15T10:00:00+00:00",
    periods: [
      {
        name: "Today",
        shortForecast: "Sunny",
        detailedForecast: "Sunny with highs in the upper 60s.",
      },
    ],
  },
};

const hourlyForecastFixture = {
  properties: {
    updated: "2026-02-15T10:00:00+00:00",
    periods: [
      {
        startTime: "2026-02-15T11:00:00+00:00",
        endTime: "2026-02-15T12:00:00+00:00",
        temperature: 42,
        temperatureUnit: "F",
        windSpeed: "8 mph",
        windDirection: "NW",
        shortForecast: "Partly Cloudy",
        detailedForecast: "Partly cloudy with a light northwest wind.",
        probabilityOfPrecipitation: { value: 10 },
        relativeHumidity: { value: 65 },
      },
      {
        startTime: "2026-02-15T12:00:00+00:00",
        endTime: "2026-02-15T13:00:00+00:00",
        temperature: 44,
        temperatureUnit: "F",
        windSpeed: "9 mph",
        windDirection: "NW",
        shortForecast: "Mostly Sunny",
        detailedForecast: "Mostly sunny with temperatures rising.",
        probabilityOfPrecipitation: { value: 5 },
        relativeHumidity: { value: 58 },
      },
    ],
  },
};

const buildHourlyPeriods = (
  startIso: string,
  hours: number,
  temperatureForHour: (hour: number) => number,
  precipitationForHour: (hour: number) => number = () => 5,
) => {
  const start = Date.parse(startIso);
  return Array.from({ length: hours }, (_, hour) => {
    const periodStart = new Date(start + hour * 60 * 60 * 1000);
    const periodEnd = new Date(start + (hour + 1) * 60 * 60 * 1000);
    return {
      startTime: periodStart.toISOString(),
      endTime: periodEnd.toISOString(),
      temperature: temperatureForHour(hour),
      temperatureUnit: "F",
      windSpeed: "10 mph",
      windDirection: "N",
      shortForecast: "Clear",
      detailedForecast: "Clear skies.",
      probabilityOfPrecipitation: {
        unitCode: "wmoUnit:percent",
        value: precipitationForHour(hour),
      },
      relativeHumidity: { value: 50 },
    };
  });
};

const createFetchMock = () => {
  const calls: FetchCall[] = [];
  const fetchMock = async (url: string, init?: RequestInit) => {
    const headers = init?.headers as Record<string, string> | undefined;
    calls.push({
      url,
      userAgent: headers?.["User-Agent"],
    });

    if (url.includes("/points/")) {
      return {
        ok: true,
        json: async () => pointsFixture,
      } as Response;
    }

    return {
      ok: true,
      json: async () => forecastFixture,
    } as Response;
  };

  return { fetchMock, calls };
};

describe("nws-forecast ingest", () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it("defaults to 7-day forecast endpoint", async () => {
    const { fetchMock, calls } = createFetchMock();

    const result = await ingest({
      lat: 36.41144,
      lon: -95.932317,
      userAgent: "weather-summarizer-test (dev@example.com)",
      fetch: fetchMock as typeof fetch,
    });

    expect(calls[0].url).toContain("/points/36.41144,-95.932317");
    expect(calls[1].url).toBe(pointsFixture.properties.forecast);
    expect(result.meta?.forecastType).toBe("forecast");
    expect(result.alerts).toHaveLength(1);
    expect(result.alerts[0].shortDescription).toContain(
      "Sunny with highs in the upper 60s.",
    );
  });

  it("supports forecastHourly forecast type", async () => {
    const { fetchMock, calls } = createFetchMock();

    const hourlyFetchMock = async (url: string, init?: RequestInit) => {
      const headers = init?.headers as Record<string, string> | undefined;
      calls.push({
        url,
        userAgent: headers?.["User-Agent"],
      });

      if (url.includes("/points/")) {
        return {
          ok: true,
          json: async () => pointsFixture,
        } as Response;
      }

      return {
        ok: true,
        json: async () => hourlyForecastFixture,
      } as Response;
    };

    const result = await ingest({
      lat: 36.4,
      lon: -95.9,
      forecastType: "forecastHourly",
      userAgent: "weather-summarizer-test (dev@example.com)",
      fetch: hourlyFetchMock as typeof fetch,
    });

    expect(calls[1].url).toBe(pointsFixture.properties.forecastHourly);
    expect(result.meta?.forecastType).toBe("forecastHourly");
  });

  it("returns compact hourly derived summary when rawMode is compact", async () => {
    const { calls } = createFetchMock();

    const hourlyFetchMock = async (url: string, init?: RequestInit) => {
      const headers = init?.headers as Record<string, string> | undefined;
      calls.push({
        url,
        userAgent: headers?.["User-Agent"],
      });

      if (url.includes("/points/")) {
        return {
          ok: true,
          json: async () => pointsFixture,
        } as Response;
      }

      return {
        ok: true,
        json: async () => hourlyForecastFixture,
      } as Response;
    };

    const result = await ingest({
      lat: 36.4,
      lon: -95.9,
      forecastType: "forecastHourly",
      rawMode: "compact",
      maxPeriods: 1,
      userAgent: "weather-summarizer-test (dev@example.com)",
      fetch: hourlyFetchMock as typeof fetch,
    });

    const raw = result.raw as Record<string, unknown>;
    const periods = raw.periods as Record<string, unknown>;

    expect(result.meta?.rawMode).toBe("compact");
    expect(periods.format).toBe("periods-derived-v1");
    expect(periods.rowCount).toBe(1);
    expect(
      (periods.metrics as Record<string, unknown>).temperature,
    ).toBeDefined();
    expect(
      (periods.nextDayDetailed as Record<string, unknown>).rainWindows,
    ).toBeDefined();
    expect(
      (periods.weekGeneralized as Record<string, unknown>).rainTrend,
    ).toBeDefined();
    expect((periods.topEvents as unknown[]).length).toBeGreaterThanOrEqual(0);
  });

  it("can omit raw payload when includeRaw is false", async () => {
    const { fetchMock } = createFetchMock();

    const result = await ingest({
      lat: 36.4,
      lon: -95.9,
      includeRaw: false,
      userAgent: "weather-summarizer-test (dev@example.com)",
      fetch: fetchMock as typeof fetch,
    });

    expect(result.raw).toBeUndefined();
  });

  it("supports forecastGridData forecast type", async () => {
    const { fetchMock, calls } = createFetchMock();

    const result = await ingest({
      lat: 36.4,
      lon: -95.9,
      forecastType: "forecastGridData",
      userAgent: "weather-summarizer-test (dev@example.com)",
      fetch: fetchMock as typeof fetch,
    });

    expect(calls[1].url).toBe(pointsFixture.properties.forecastGridData);
    expect(result.meta?.forecastType).toBe("forecastGridData");
  });

  it("sends User-Agent header on NWS requests", async () => {
    const { fetchMock, calls } = createFetchMock();

    await ingest({
      lat: 36.4,
      lon: -95.9,
      userAgent: "weather-summarizer-test (dev@example.com)",
      fetch: fetchMock as typeof fetch,
    });

    expect(calls).toHaveLength(2);
    expect(calls[0].userAgent).toBe(
      "weather-summarizer-test (dev@example.com)",
    );
    expect(calls[1].userAgent).toBe(
      "weather-summarizer-test (dev@example.com)",
    );
  });

  it("throws when userAgent is missing", async () => {
    const { fetchMock } = createFetchMock();

    await expect(
      ingest({
        lat: 36.4,
        lon: -95.9,
        fetch: fetchMock as typeof fetch,
      }),
    ).rejects.toThrow(/userAgent is required/i);
  });

  it("derives majorTemperatureShifts from full future-day highs/lows and excludes current day", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-02-15T08:00:00.000Z"));

    const periods = buildHourlyPeriods(
      "2026-02-15T00:00:00.000Z",
      96,
      (hour) => {
        if (hour < 24) {
          return 48;
        }
        if (hour < 48) {
          // 2026-02-16: low 36, high 50
          return hour % 24 < 12 ? 36 : 50;
        }
        if (hour < 72) {
          // 2026-02-17: low 37, high 51
          return hour % 24 < 12 ? 37 : 51;
        }
        // 2026-02-18: low 45, high 63
        return hour % 24 < 12 ? 45 : 63;
      },
    );

    const hourlyFetchMock = async (url: string) => {
      if (url.includes("/points/")) {
        return {
          ok: true,
          json: async () => pointsFixture,
        } as Response;
      }

      return {
        ok: true,
        json: async () => ({
          properties: {
            updated: "2026-02-15T10:00:00+00:00",
            periods,
          },
        }),
      } as Response;
    };

    const result = await ingest({
      lat: 36.4,
      lon: -95.9,
      forecastType: "forecastHourly",
      rawMode: "compact",
      userAgent: "weather-summarizer-test (dev@example.com)",
      fetch: hourlyFetchMock as typeof fetch,
    });

    const raw = result.raw as Record<string, unknown>;
    const periodsDerived = raw.periods as Record<string, unknown>;
    const weekGeneralized = periodsDerived.weekGeneralized as Record<
      string,
      unknown
    >;
    const nextDayDetailed = periodsDerived.nextDayDetailed as Record<
      string,
      unknown
    >;
    const temperature = weekGeneralized.temperature as Record<string, unknown>;
    const majorShifts = temperature.majorTemperatureShifts as Array<
      Record<string, unknown>
    >;
    const nextDayMajorShifts = nextDayDetailed.majorTemperatureShifts as Array<
      Record<string, unknown>
    >;

    expect(nextDayMajorShifts).toHaveLength(0);

    expect(majorShifts).toHaveLength(1);
    expect(majorShifts[0]).toMatchObject({
      description: "warming high and low from 2026-02-17 to 2026-02-18",
      highDelta: 12,
      lowDelta: 8,
    });
  });

  it("classifies majorTemperatureShifts contributor and direction for low-only changes", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-02-15T08:00:00.000Z"));

    const periods = buildHourlyPeriods(
      "2026-02-17T00:00:00.000Z",
      72,
      (hour) => {
        if (hour < 24) {
          // 2026-02-17 baseline: low 40, high 55
          return hour % 24 < 12 ? 40 : 55;
        }
        if (hour < 48) {
          // 2026-02-18: high-only cooling (high 45, low 39)
          return hour % 24 < 12 ? 39 : 45;
        }
        // 2026-02-19: low-only warming (high 47, low 48)
        return hour % 24 < 12 ? 48 : 47;
      },
    );

    const hourlyFetchMock = async (url: string) => {
      if (url.includes("/points/")) {
        return {
          ok: true,
          json: async () => pointsFixture,
        } as Response;
      }

      return {
        ok: true,
        json: async () => ({
          properties: {
            updated: "2026-02-15T10:00:00+00:00",
            periods,
          },
        }),
      } as Response;
    };

    const result = await ingest({
      lat: 36.4,
      lon: -95.9,
      forecastType: "forecastHourly",
      rawMode: "compact",
      userAgent: "weather-summarizer-test (dev@example.com)",
      fetch: hourlyFetchMock as typeof fetch,
    });

    const raw = result.raw as Record<string, unknown>;
    const periodsDerived = raw.periods as Record<string, unknown>;
    const weekGeneralized = periodsDerived.weekGeneralized as Record<
      string,
      unknown
    >;
    const temperature = weekGeneralized.temperature as Record<string, unknown>;
    const majorShifts = temperature.majorTemperatureShifts as Array<
      Record<string, unknown>
    >;

    expect(majorShifts).toHaveLength(1);
    expect(majorShifts[0]).toMatchObject({
      description: "warming low from 2026-02-18 to 2026-02-19",
      highDelta: 3,
      lowDelta: 8,
    });
  });

  it("classifies majorTemperatureShifts contributor and direction for high-only changes", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-02-15T08:00:00.000Z"));

    const periods = buildHourlyPeriods(
      "2026-02-15T00:00:00.000Z",
      96,
      (hour) => {
        if (hour < 24) {
          return 50;
        }
        if (hour < 48) {
          // 2026-02-16 baseline: low 40, high 54
          return hour % 24 < 12 ? 40 : 54;
        }
        if (hour < 72) {
          // 2026-02-17 baseline: low 41, high 55
          return hour % 24 < 12 ? 41 : 55;
        }
        // 2026-02-18: high-only cooling (high 45, low 42)
        return hour % 24 < 12 ? 42 : 45;
      },
    );

    const hourlyFetchMock = async (url: string) => {
      if (url.includes("/points/")) {
        return {
          ok: true,
          json: async () => pointsFixture,
        } as Response;
      }

      return {
        ok: true,
        json: async () => ({
          properties: {
            updated: "2026-02-15T10:00:00+00:00",
            periods,
          },
        }),
      } as Response;
    };

    const result = await ingest({
      lat: 36.4,
      lon: -95.9,
      forecastType: "forecastHourly",
      rawMode: "compact",
      userAgent: "weather-summarizer-test (dev@example.com)",
      fetch: hourlyFetchMock as typeof fetch,
    });

    const raw = result.raw as Record<string, unknown>;
    const periodsDerived = raw.periods as Record<string, unknown>;
    const weekGeneralized = periodsDerived.weekGeneralized as Record<
      string,
      unknown
    >;
    const temperature = weekGeneralized.temperature as Record<string, unknown>;
    const majorShifts = temperature.majorTemperatureShifts as Array<
      Record<string, unknown>
    >;

    expect(majorShifts).toHaveLength(1);
    expect(majorShifts[0]).toMatchObject({
      description: "cooling high from 2026-02-17 to 2026-02-18",
      highDelta: -10,
      lowDelta: 1,
    });
  });

  it("does not emit majorTemperatureShifts when full-day buckets are not consecutive", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-02-15T08:00:00.000Z"));

    const day17 = buildHourlyPeriods("2026-02-17T00:00:00.000Z", 24, (hour) =>
      hour < 12 ? 30 : 50,
    );
    const day19 = buildHourlyPeriods("2026-02-19T00:00:00.000Z", 24, (hour) =>
      hour < 12 ? 48 : 66,
    );
    const periods = [...day17, ...day19];

    const hourlyFetchMock = async (url: string) => {
      if (url.includes("/points/")) {
        return {
          ok: true,
          json: async () => pointsFixture,
        } as Response;
      }

      return {
        ok: true,
        json: async () => ({
          properties: {
            updated: "2026-02-15T10:00:00+00:00",
            periods,
          },
        }),
      } as Response;
    };

    const result = await ingest({
      lat: 36.4,
      lon: -95.9,
      forecastType: "forecastHourly",
      rawMode: "compact",
      userAgent: "weather-summarizer-test (dev@example.com)",
      fetch: hourlyFetchMock as typeof fetch,
    });

    const raw = result.raw as Record<string, unknown>;
    const periodsDerived = raw.periods as Record<string, unknown>;
    const weekGeneralized = periodsDerived.weekGeneralized as Record<
      string,
      unknown
    >;
    const temperature = weekGeneralized.temperature as Record<string, unknown>;
    const majorShifts = temperature.majorTemperatureShifts as Array<
      Record<string, unknown>
    >;

    expect(majorShifts).toHaveLength(0);
  });

  it("does not include temperature_peak or temperature_valley in topEvents", async () => {
    const periods = buildHourlyPeriods(
      "2026-02-15T00:00:00.000Z",
      30,
      (hour) => (hour % 2 === 0 ? 40 : 60),
    );

    const hourlyFetchMock = async (url: string) => {
      if (url.includes("/points/")) {
        return {
          ok: true,
          json: async () => pointsFixture,
        } as Response;
      }

      return {
        ok: true,
        json: async () => ({
          properties: {
            updated: "2026-02-15T10:00:00+00:00",
            periods,
          },
        }),
      } as Response;
    };

    const result = await ingest({
      lat: 36.4,
      lon: -95.9,
      forecastType: "forecastHourly",
      rawMode: "compact",
      userAgent: "weather-summarizer-test (dev@example.com)",
      fetch: hourlyFetchMock as typeof fetch,
    });

    const raw = result.raw as Record<string, unknown>;
    const periodsDerived = raw.periods as Record<string, unknown>;
    const topEvents = periodsDerived.topEvents as Array<
      Record<string, unknown>
    >;
    const eventTypes = topEvents.map((event) => event.type);

    expect(eventTypes).not.toContain("temperature_peak");
    expect(eventTypes).not.toContain("temperature_valley");
  });

  it("adds rain trend increase start dates for each new increasing run", async () => {
    const periods = buildHourlyPeriods(
      "2026-02-15T00:00:00.000Z",
      5 * 24,
      () => 50,
      (hour) => {
        if (hour < 24) {
          return 10;
        }
        if (hour < 48) {
          return 20;
        }
        if (hour < 72) {
          return 30;
        }
        if (hour < 96) {
          return 18;
        }
        return 30;
      },
    );

    const hourlyFetchMock = async (url: string) => {
      if (url.includes("/points/")) {
        return {
          ok: true,
          json: async () => pointsFixture,
        } as Response;
      }

      return {
        ok: true,
        json: async () => ({
          properties: {
            updated: "2026-02-15T10:00:00+00:00",
            periods,
          },
        }),
      } as Response;
    };

    const result = await ingest({
      lat: 36.4,
      lon: -95.9,
      forecastType: "forecastHourly",
      rawMode: "compact",
      userAgent: "weather-summarizer-test (dev@example.com)",
      fetch: hourlyFetchMock as typeof fetch,
    });

    const raw = result.raw as Record<string, unknown>;
    const periodsDerived = raw.periods as Record<string, unknown>;
    const weekGeneralized = periodsDerived.weekGeneralized as Record<
      string,
      unknown
    >;
    const temperature = weekGeneralized.temperature as Record<string, unknown>;
    const rainTrend = weekGeneralized.rainTrend as Record<string, unknown>;
    const increaseStartDates = rainTrend.increaseStartDates as string[];
    const dailyHighLow = temperature.dailyHighLow as Array<
      Record<string, unknown>
    >;

    expect(increaseStartDates).toEqual(["2026-02-16", "2026-02-19"]);
    expect(temperature.min).toBeUndefined();
    expect(temperature.max).toBeUndefined();
    expect(temperature.dailyAverages).toBeUndefined();
    expect(dailyHighLow.length).toBeGreaterThan(0);
    expect(dailyHighLow[0]).toHaveProperty("day");
    expect(dailyHighLow[0]).toHaveProperty("high");
    expect(dailyHighLow[0]).toHaveProperty("low");
  });
});
