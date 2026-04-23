import { describe, expect, it } from "vitest";
import {
  buildShortDescriptionFromDescription,
  defaultLogger,
  extractPreText,
  hashDedupeKey,
  pointInPolygon,
  resolveFetch,
  resolveNow,
  toIso,
} from "../src/index";

describe("ingest-core utilities", () => {
  it("hashDedupeKey returns deterministic SHA256 hashes", () => {
    const first = hashDedupeKey("example-key");
    const second = hashDedupeKey("example-key");

    expect(first).toBe(second);
    expect(first).toMatch(/^[a-f0-9]{64}$/);
  });

  it("pointInPolygon identifies points inside and outside polygon", () => {
    const polygon = [
      [-96, 35],
      [-96, 37],
      [-94, 37],
      [-94, 35],
      [-96, 35],
    ];

    expect(pointInPolygon([-95, 36], polygon)).toBe(true);
    expect(pointInPolygon([-97, 36], polygon)).toBe(false);
  });

  it("toIso parses valid values and returns undefined for invalid values", () => {
    const fromString = toIso("2026-02-15T12:00:00Z");
    const fromDate = toIso(new Date("2026-02-15T12:00:00Z"));

    expect(fromString?.toISOString()).toBe("2026-02-15T12:00:00.000Z");
    expect(fromDate?.toISOString()).toBe("2026-02-15T12:00:00.000Z");
    expect(toIso("not-a-date")).toBeUndefined();
    expect(toIso(undefined)).toBeUndefined();
    expect(toIso(null)).toBeUndefined();
  });

  it("resolveNow uses override when provided", () => {
    const expected = new Date("2026-02-15T13:30:00Z");

    const now = resolveNow({
      lat: 0,
      lon: 0,
      now: () => expected,
    });

    expect(now).toBe(expected);
  });

  it("resolveFetch uses config.fetch when available", () => {
    const mockFetch = async () => ({ ok: true }) as Response;

    const resolved = resolveFetch({
      lat: 0,
      lon: 0,
      fetch: mockFetch as typeof fetch,
    });

    expect(resolved).toBe(mockFetch);
  });

  it("defaultLogger methods are callable", () => {
    expect(() => defaultLogger.info("message")).not.toThrow();
    expect(() => defaultLogger.warn("message")).not.toThrow();
    expect(() => defaultLogger.error("message")).not.toThrow();
  });

  it("extractPreText returns combined pre content", () => {
    const result = extractPreText(
      "<div><pre>First</pre><pre>Second</pre></div>",
    );
    expect(result).toBe("First\nSecond");
  });

  it("buildShortDescriptionFromDescription prefers pre and strips LAT...LON", () => {
    const result = buildShortDescriptionFromDescription(
      "<pre>SPC MD\nLAT...LON 3600 9700 3500 9600\nStorms expected</pre>",
      {
        preferPreText: true,
        stripLatLonCoordinates: true,
      },
    );

    expect(result).toContain("SPC MD");
    expect(result).toContain("Storms expected");
    expect(result).not.toContain("LAT...LON");
  });
});
