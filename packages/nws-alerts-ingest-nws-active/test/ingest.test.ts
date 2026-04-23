import { describe, expect, it } from "vitest";
import { ingest } from "../src/index";
import fixture from "./fixtures/active.json";

const fetchImpl = async () => {
  return {
    ok: true,
    json: async () => fixture,
  } as Response;
};

describe("nws-active ingest", () => {
  it("maps features to alerts", async () => {
    const result = await ingest({
      lat: 36.0,
      lon: -95.0,
      fetch: fetchImpl as typeof fetch,
    });

    expect(result.alerts).toHaveLength(1);
    expect(result.alerts[0].nwsId).toBe("test-id-1");
    expect(result.alerts[0].shortDescription).toBeDefined();
    expect(result.alerts[0].shortDescription?.length).toBeGreaterThan(0);
    expect(result.meta?.featureCount).toBe(1);
  });
});
