import { describe, expect, it } from "vitest";
import { ingest } from "../src/index";

const geojson = {
  type: "FeatureCollection",
  features: [
    {
      type: "Feature",
      properties: {
        LABEL: "SLGT",
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
    },
    {
      type: "Feature",
      properties: {
        LABEL: "HIGH",
      },
      geometry: {
        type: "Polygon",
        coordinates: [
          [
            [-120, 45],
            [-120, 46],
            [-119, 46],
            [-119, 45],
            [-120, 45],
          ],
        ],
      },
    },
  ],
};

const html = "<html><body>SPC Day 1</body></html>";

const fetchImpl = async (url: string) => {
  if (url.endsWith(".geojson")) {
    return {
      ok: true,
      json: async () => geojson,
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
});
