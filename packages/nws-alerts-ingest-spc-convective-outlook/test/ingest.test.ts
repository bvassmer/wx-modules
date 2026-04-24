import { describe, expect, it } from "vitest";
import { ingest } from "../src/index";

const buildFeatureCollection = (labels: string | string[]) => ({
  type: "FeatureCollection",
  features: (Array.isArray(labels) ? labels : [labels]).map((label) => ({
    type: "Feature",
    properties: {
      LABEL: label,
    },
    geometry: {
      type: "Polygon",
      coordinates: [
        [
          [-96, 35],
          [-96, 37],
          [-94, 37],
          [-94, 35],
          [-96, 35],
        ],
      ],
    },
  })),
});

const html = "<html><body>SPC Day 1</body></html>";

const fetchImpl = async (url: string) => {
  if (url.endsWith(".geojson")) {
    return {
      ok: true,
      json: async () => buildFeatureCollection("SLGT"),
    } as Response;
  }
  if (url.endsWith(".html")) {
    return {
      ok: true,
      text: async () => html,
    } as Response;
  }
  return { ok: false, status: 404 } as Response;
};

describe("spc-convective-outlook ingest", () => {
  it("creates an alert for day1 during issue window", async () => {
    const result = await ingest({
      lat: 36.0,
      lon: -95.0,
      fetch: fetchImpl as typeof fetch,
      now: () => new Date("2024-06-01T12:30:00Z"),
    });

    expect(result.alerts.length).toBeGreaterThan(0);
    expect(result.alerts[0].event).toBe("SPC Convective Outlook Day 1");
    expect(result.alerts[0].shortDescription).toContain("SPC Day 1");
    expect((result.alerts[0].extra as any)?.location).toEqual({
      lat: 36,
      lon: -95,
    });
    expect((result.raw as any)?.day1?.cat?.features).toHaveLength(1);
    expect((result.meta as any)?.sourceLocation).toEqual({
      lat: 36,
      lon: -95,
    });
  });

  it("uses Day 1 CIG layer URLs and maps local CIG levels to star counts", async () => {
    const result = await ingest({
      lat: 36.0,
      lon: -95.0,
      fetch: (async (url: string) => {
        if (url.endsWith(".html")) {
          return {
            ok: true,
            text: async () => html,
          } as Response;
        }
        if (url.includes("day1") && url.includes("_cat")) {
          return {
            ok: true,
            json: async () => buildFeatureCollection("SLGT"),
          } as Response;
        }
        if (url.includes("day1") && url.includes("_torn")) {
          return {
            ok: true,
            json: async () => buildFeatureCollection(["0.05", "CIG1"]),
          } as Response;
        }
        if (url.includes("day1") && url.includes("_hail")) {
          return {
            ok: true,
            json: async () => buildFeatureCollection(["0.15", "CIG1"]),
          } as Response;
        }
        if (url.includes("day1") && url.includes("_wind")) {
          return {
            ok: true,
            json: async () => buildFeatureCollection(["0.30", "CIG1"]),
          } as Response;
        }
        if (url.includes("day1") && url.includes("cigtorn")) {
          return {
            ok: true,
            json: async () => buildFeatureCollection(["CIG1", "CIG3"]),
          } as Response;
        }
        if (url.includes("day1") && url.includes("cighail")) {
          return {
            ok: true,
            json: async () => buildFeatureCollection(["CIG1", "CIG2"]),
          } as Response;
        }
        if (url.includes("day1") && url.includes("cigwind")) {
          return {
            ok: true,
            json: async () => buildFeatureCollection("CIG1"),
          } as Response;
        }
        return {
          ok: true,
          json: async () => buildFeatureCollection("MRGL"),
        } as Response;
      }) as typeof fetch,
      now: () => new Date("2024-06-01T12:30:00Z"),
    });

    const day1Alert = result.alerts.find(
      (alert) => alert.event === "SPC Convective Outlook Day 1",
    );

    expect(day1Alert?.headline).toBe(
      "SPC Conv Day 1 - SLGT T0.05*** H0.15** W0.30*",
    );
    expect((day1Alert?.extra as any)?.urls?.sigtorn).toContain("cigtorn");
    expect((day1Alert?.extra as any)?.urls?.sighail).toContain("cighail");
    expect((day1Alert?.extra as any)?.urls?.sigwind).toContain("cigwind");
  });

  it("maps Day 3 CIG levels to repeated stars in the subject", async () => {
    const result = await ingest({
      lat: 36.0,
      lon: -95.0,
      fetch: (async (url: string) => {
        if (url.endsWith(".html")) {
          return {
            ok: true,
            text: async () => html,
          } as Response;
        }
        if (url.includes("day3") && url.includes("_cat")) {
          return {
            ok: true,
            json: async () => buildFeatureCollection("ENH"),
          } as Response;
        }
        if (url.includes("day3") && url.includes("_sigprob")) {
          return {
            ok: true,
            json: async () => buildFeatureCollection(["CIG1", "CIG2"]),
          } as Response;
        }
        if (url.includes("day3") && url.includes("_prob")) {
          return {
            ok: true,
            json: async () => buildFeatureCollection("0.30"),
          } as Response;
        }
        return {
          ok: true,
          json: async () => buildFeatureCollection("MRGL"),
        } as Response;
      }) as typeof fetch,
      now: () => new Date("2024-06-01T07:30:00Z"),
    });

    const day3Alert = result.alerts.find(
      (alert) => alert.event === "SPC Convective Outlook Day 3",
    );

    expect(day3Alert?.headline).toBe("SPC Conv Day 3 - ENH P0.30**");
  });
});
