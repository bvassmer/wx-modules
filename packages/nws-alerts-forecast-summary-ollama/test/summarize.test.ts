import { describe, expect, it, vi } from "vitest";
import { ForecastVariable, summarizeForecastVariable } from "../src/index";

describe("summarizeForecastVariable", () => {
  it("calls ollama and returns one sentence", async () => {
    const fetchMock = vi.fn(async () => {
      return {
        ok: true,
        json: async () => ({
          response:
            "Temperatures trend upward through the period with a steady late-day warming signal. Additional sentence.",
        }),
      } as Response;
    });

    const result = await summarizeForecastVariable(
      {
        variable: ForecastVariable.Temperature,
        unit: "F",
        points: [
          { date: "2026-02-15T12:00:00Z", value: 47 },
          { date: "2026-02-15T10:00:00Z", value: 42 },
          { date: "2026-02-15T11:00:00Z", value: 45 },
        ],
      },
      {
        fetchImpl: fetchMock as unknown as typeof fetch,
      },
    );

    expect(result).toBe(
      "Temperatures trend upward through the period with a steady late-day warming signal.",
    );

    expect(fetchMock).toHaveBeenCalledTimes(1);
    const firstCall = fetchMock.mock.calls.at(0);
    expect(firstCall).toBeDefined();
    const [url, init] = firstCall as unknown as [string, RequestInit];
    expect(url).toBe("http://localhost:11434/api/generate");
    expect(init.method).toBe("POST");

    const body = JSON.parse(String(init.body)) as {
      model: string;
      stream: boolean;
      prompt: string;
    };

    expect(body.model).toBe("llama3.1");
    expect(body.stream).toBe(false);
    expect(body.prompt).toContain("Forecast variable: temperature");
    expect(body.prompt).toContain(
      '[{"date":"2026-02-15T10:00:00.000Z","value":42}',
    );
  });

  it("throws when no points are provided", async () => {
    await expect(
      summarizeForecastVariable({
        variable: ForecastVariable.WindSpeed,
        points: [],
      }),
    ).rejects.toThrow("points must include at least one date/value pair");
  });
});
