import { describe, expect, it } from "vitest";
import { ingest } from "../src/index";

const kml = `<?xml version="1.0" encoding="UTF-8"?>
<kml>
  <Document>
    <Folder>
      <Placemark>
        <name>10%</name>
        <Polygon>
          <outerBoundaryIs>
            <LinearRing>
              <coordinates>
                -96,35 -96,37 -94,37 -94,35 -96,35
              </coordinates>
            </LinearRing>
          </outerBoundaryIs>
        </Polygon>
      </Placemark>
    </Folder>
  </Document>
</kml>`;

const createResponse = (body: string, status = 200): Response => {
  const buf = Buffer.from(body, "utf-8");
  return {
    ok: status >= 200 && status < 300,
    status,
    arrayBuffer: async () =>
      buf.buffer.slice(buf.byteOffset, buf.byteOffset + buf.byteLength),
  } as Response;
};

const createFetch = (handler: (url: string) => Response | Promise<Response>) =>
  (async (url: string) => handler(url)) as typeof fetch;

describe("wpc-snow-pwpf ingest", () => {
  it("returns alerts when forecasts are found", async () => {
    const result = await ingest({
      lat: 36.0,
      lon: -95.0,
      fetch: createFetch(() => createResponse(kml)),
      now: () => new Date("2024-12-01T12:00:00Z"),
    });

    expect(result.alerts.length).toBeGreaterThan(0);
    expect(result.alerts[0].event).toBe("WPC Snow Forecast");
    expect(result.alerts[0].shortDescription).toContain(
      "WPC Probabilistic Winter Precipitation Forecast",
    );
    expect(result.alerts[0].shortDescription).toContain("Probability outlooks");
    expect(result.alerts[0].shortDescription).toContain("12h ≥2in (f012): 10%");
    expect((result.alerts[0].extra as any)?.location).toEqual({
      lat: 36,
      lon: -95,
    });
    expect((result.meta as any)?.sourceLocation).toEqual({
      lat: 36,
      lon: -95,
    });
  });

  it("requests the live 12h ge02 probability product instead of ge01", async () => {
    const requestUrls: string[] = [];

    await ingest({
      lat: 36.0,
      lon: -95.0,
      fetch: createFetch((url) => {
        requestUrls.push(String(url));
        return createResponse(kml);
      }),
      now: () => new Date("2024-12-01T12:00:00Z"),
    });

    expect(requestUrls[0]).toContain("prb_12hsnow_ge02_f012_latest_GE.kmz");
    expect(
      requestUrls.some((url) =>
        url.includes("prb_12hsnow_ge01_f012_latest_GE"),
      ),
    ).toBe(false);
  });

  it("does not attempt KML fallbacks after 404 KMZ misses", async () => {
    const requestUrls: string[] = [];

    const result = await ingest({
      lat: 36.0,
      lon: -95.0,
      fetch: createFetch((url) => {
        requestUrls.push(String(url));
        return createResponse("", 404);
      }),
      now: () => new Date("2024-12-01T12:00:00Z"),
    });

    expect(result.alerts).toEqual([]);
    expect(requestUrls.some((url) => url.endsWith(".kml"))).toBe(false);
    expect(requestUrls[0]).toContain("prb_12hsnow_ge02_f012_latest_GE.kmz");
  });

  it("falls back to KML once after a non-404 KMZ failure", async () => {
    const requestUrls: string[] = [];

    const result = await ingest({
      lat: 36.0,
      lon: -95.0,
      fetch: createFetch((url) => {
        const requestUrl = String(url);
        requestUrls.push(requestUrl);

        if (requestUrl.includes("prb_24hsnow_ge01_f024_latest_GE.kmz")) {
          return createResponse("upstream error", 500);
        }

        if (requestUrl.includes("prb_24hsnow_ge01_f024_latest_GE.kml")) {
          return createResponse(kml);
        }

        return createResponse("", 404);
      }),
      now: () => new Date("2024-12-01T12:00:00Z"),
    });

    const kmlRequests = requestUrls.filter((url) => url.endsWith(".kml"));

    expect(result.alerts.length).toBe(1);
    expect(result.alerts[0].shortDescription).toContain("24h ≥1in (f024): 10%");
    expect(kmlRequests).toHaveLength(1);
    expect(kmlRequests[0]).toContain("prb_24hsnow_ge01_f024_latest_GE.kml");
  });
});
