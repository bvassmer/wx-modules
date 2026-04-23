import { describe, expect, it } from "vitest";
import { ingest } from "../src/index";

const geojson = {
  type: "FeatureCollection",
  features: [
    {
      type: "Feature",
      properties: { LABEL: "SLGT" },
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
      properties: { LABEL: "HIGH" },
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

const fetchImpl = async () => {
  return {
    ok: true,
    json: async () => geojson,
  } as Response;
};

describe("wpc-excessive-rainfall ingest", () => {
  it("returns alerts during issuance window", async () => {
    const result = await ingest({
      lat: 36.0,
      lon: -95.0,
      fetch: fetchImpl as typeof fetch,
      now: () => new Date("2024-06-01T08:45:00Z"),
    });

    expect(result.alerts.length).toBeGreaterThan(0);
    expect((result.alerts[0].extra as any)?.location).toEqual({
      lat: 36,
      lon: -95,
    });
    expect(result.alerts[0].shortDescription).toContain(
      "WPC Excessive Rainfall Outlook",
    );
    expect(result.alerts[0].shortDescription).toContain("SLGT");
    expect((result.raw as any)?.products?.[0]?.featureCount).toBe(1);
    expect((result.raw as any)?.products?.[0]?.labels).toContain("SLGT");
    expect((result.meta as any)?.sourceLocation).toEqual({
      lat: 36,
      lon: -95,
    });
  });
});
