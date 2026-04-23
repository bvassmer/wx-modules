import { describe, expect, it } from "vitest";
import {
  renderChartDataUri,
  renderChartPng,
  renderLineGraphDataUri,
  renderLineGraphPng,
  toInlinePngDataUri,
} from "../src/index";

describe("nws-alerts-graphing", () => {
  const datetimes = [
    "2026-02-15T00:00:00Z",
    "2026-02-15T03:00:00Z",
    "2026-02-15T06:00:00Z",
    "2026-02-15T09:00:00Z",
  ];

  it("renders a PNG for one series (legacy)", async () => {
    const png = await renderLineGraphPng({
      datetimes,
      title: "Temperature",
      yAxisLabel: "F",
      xAxisLabel: "Time (UTC)",
      series: [{ values: [38, 36, 35, 37], label: "Temp" }],
    });

    expect(png.length).toBeGreaterThan(1000);
    expect(png.subarray(0, 8).equals(Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]))).toBe(true);
  });

  it("renders a PNG for two series (legacy)", async () => {
    const png = await renderLineGraphPng({
      datetimes,
      series: [
        { values: [42, 43, 44, 45], label: "Temp" },
        { values: [36, 45, 52, 40], label: "Humidity" },
      ],
    });

    expect(png.length).toBeGreaterThan(1000);
  });

  it("renders a PNG for 3+ series", async () => {
    const png = await renderChartPng({
      datetimes,
      series: [
        { values: [1, 2, 3, 4], label: "A" },
        { values: [2, 3, 4, 5], label: "B" },
        { values: [3, 4, 5, 6], label: "C" },
      ],
    });

    expect(png.length).toBeGreaterThan(1000);
  });

  it("renders each chart type", async () => {
    const base = {
      datetimes,
      series: [{ values: [1, 2, 3, 4], label: "Series" }],
    };

    const line = await renderChartPng({ ...base, chartType: "line" });
    const bar = await renderChartPng({ ...base, chartType: "bar" });
    const area = await renderChartPng({ ...base, chartType: "area" });
    const stepped = await renderChartPng({ ...base, chartType: "stepped" });

    expect(line.length).toBeGreaterThan(1000);
    expect(bar.length).toBeGreaterThan(1000);
    expect(area.length).toBeGreaterThan(1000);
    expect(stepped.length).toBeGreaterThan(1000);
  });

  it("supports null values with gaps", async () => {
    const png = await renderChartPng({
      datetimes,
      series: [{ values: [1, null, 3, null], label: "Nulls" }],
      nullHandling: "gap",
    });

    expect(png.length).toBeGreaterThan(1000);
  });

  it("supports null interpolation", async () => {
    const png = await renderChartPng({
      datetimes,
      series: [{ values: [1, null, 3, null], label: "Nulls" }],
      nullHandling: "interpolate",
    });

    expect(png.length).toBeGreaterThan(1000);
  });

  it("supports axis options and styling", async () => {
    const png = await renderChartPng({
      datetimes,
      series: [{ values: [1, 2, 3, 4], label: "Styled" }],
      xAxis: { label: "Time", tickRotation: 45, tickMax: 6, dateFormat: { format: "HH:mm" } },
      yAxis: { label: "Value", min: 0, max: 5, tickStepSize: 1 },
      style: { fontFamily: "Arial", fontSize: 12, gridColor: "#e5e7eb", plotAreaColor: "#f8fafc" },
    });

    expect(png.length).toBeGreaterThan(1000);
  });

  it("supports annotations", async () => {
    const png = await renderChartPng({
      datetimes,
      series: [{ values: [1, 2, 3, 4], label: "Series" }],
      annotations: {
        thresholds: [{ y: 2.5, label: "Severe" }],
        markers: [{ x: datetimes[1], label: "Issued" }],
        ranges: [{ start: datetimes[0], end: datetimes[2], label: "Window" }],
      },
    });

    expect(png.length).toBeGreaterThan(1000);
  });

  it("supports legend truncation options", async () => {
    const png = await renderChartPng({
      datetimes,
      series: [{ values: [1, 2, 3, 4], label: "VeryLongLegendLabelThatShouldTruncate" }],
      legend: { maxLabelLength: 8 },
    });

    expect(png.length).toBeGreaterThan(1000);
  });

  it("builds a data URI from PNG", () => {
    const uri = toInlinePngDataUri(Buffer.from("test"));
    expect(uri).toBe("data:image/png;base64,dGVzdA==");
  });

  it("renders a data URI", async () => {
    const uri = await renderLineGraphDataUri({
      datetimes,
      series: [{ values: [1, 2, 3, 4], label: "Example" }],
    });

    expect(uri.startsWith("data:image/png;base64,")).toBe(true);
    expect(uri.length).toBeGreaterThan(500);
  });

  it("renders a data URI with new API", async () => {
    const uri = await renderChartDataUri({
      datetimes,
      series: [{ values: [1, 2, 3, 4], label: "Example" }],
    });

    expect(uri.startsWith("data:image/png;base64,")).toBe(true);
    expect(uri.length).toBeGreaterThan(500);
  });

  it("throws for mismatched series length", async () => {
    await expect(
      renderChartPng({
        datetimes,
        series: [{ values: [1, 2, 3], label: "Short" }],
      }),
    ).rejects.toThrow(/must match datetimes length/);
  });
});
