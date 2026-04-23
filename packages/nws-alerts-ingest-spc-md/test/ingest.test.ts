import { describe, expect, it } from "vitest";
import { ingest } from "../src/index";
import fs from "fs";

const fixtureUrl = new URL("./fixtures/md.xml", import.meta.url);
const fixture = fs.readFileSync(fixtureUrl, "utf-8");

const fetchImpl = async () => {
  return {
    ok: true,
    text: async () => fixture,
  } as Response;
};

describe("spc-md ingest", () => {
  it("parses MD RSS and filters by point-in-polygon", async () => {
    const result = await ingest({
      lat: 36.3,
      lon: -95.5,
      fetch: fetchImpl as typeof fetch,
    });

    expect(result.alerts).toHaveLength(1);
    expect(result.alerts[0].nwsId).toBe("md-1234");
    expect(result.alerts[0].shortDescription).toBeDefined();
    expect(result.alerts[0].shortDescription).not.toContain("LAT...LON");
    expect((result.alerts[0].extra as any)?.location).toEqual({
      lat: 36.3,
      lon: -95.5,
    });
    expect(result.raw?.items).toHaveLength(1);
    expect((result.meta as any)?.sourceLocation).toEqual({
      lat: 36.3,
      lon: -95.5,
    });
  });
});
