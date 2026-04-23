# nws-alerts-graphing

Utilities for generating PNG line/bar/area charts for weather email content.

## Install (workspace)

This package is included via the root npm workspace.

## Usage

```ts
import {
  renderChartDataUri,
  renderChartPng,
  renderLineGraphDataUri,
  renderLineGraphPng,
} from "nws-alerts-graphing";

const datetimes = [
  "2026-02-15T00:00:00Z",
  "2026-02-15T03:00:00Z",
  "2026-02-15T06:00:00Z",
];

const pngBuffer = await renderChartPng({
  datetimes,
  title: "Forecast Trend",
  chartType: "line",
  xAxis: { label: "Time (UTC)" },
  yAxis: { label: "Value" },
  series: [
    { label: "Temperature", values: [37, 35, 36] },
    { label: "Wind", values: [12, 18, 15] },
  ],
});

const dataUri = await renderChartDataUri({
  datetimes,
  chartType: "bar",
  series: [{ label: "Alerts", values: [3, 6, 4] }],
});

// Legacy API (line charts only):
const legacyPng = await renderLineGraphPng({
  datetimes,
  series: [{ label: "Temperature", values: [37, 35, 36] }],
});

const legacyDataUri = await renderLineGraphDataUri({
  datetimes,
  series: [{ label: "Temperature", values: [37, 35, 36] }],
});
```

## API

### New API

- `renderChartPng(input)`
  - Returns a PNG `Buffer`.
- `renderChartDataUri(input)`
  - Returns a `data:image/png;base64,...` string ready for HTML `<img src="...">`.

`RenderChartInput`:

- `datetimes`: array of Date-compatible values (`Date | string | number`).
- `series`: 1+ series objects. Each series must match `datetimes.length` and can include `null` values.
- `chartType`: `"line" | "bar" | "area" | "stepped"` (default `"line"`).
- `areaStacked`: boolean (only used for `"area"`).
- `nullHandling`: `"gap" | "interpolate"` (default `"gap"`).
- `palette`: array of hex colors for series; overridden by `series.lineColor`.
- `legend.maxLabelLength`: truncate long legend labels (default `24`, set `0` to disable).
- `xAxis` / `yAxis`: axis labels and tick controls.
  - `xAxis.dateFormat.format`: date-fns format string (default `"MMM dd, HH:mm"`).
  - `xAxis.dateFormat.timeZone`: timezone (default `"UTC"`).
- `style`: fonts, grid, plot area, line/point sizes.
- `annotations`: thresholds, markers, shaded ranges.

### Legacy API

- `renderLineGraphPng(input)`
- `renderLineGraphDataUri(input)`
- `toInlinePngDataUri(buffer)`

`RenderGraphInput` (legacy):

- `datetimes`: array of Date-compatible values.
- `series`: 1+ series objects.
- Optional: `title`, `xAxisLabel`, `yAxisLabel`, `width`, `height`, `backgroundColor`.

## Examples

### Area Chart With Stacking

```ts
await renderChartPng({
  datetimes,
  chartType: "area",
  areaStacked: true,
  series: [
    { label: "Advisory", values: [2, 3, 1] },
    { label: "Warning", values: [1, 1, 2] },
  ],
});
```

### Stepped Lines For Alert State

```ts
await renderChartPng({
  datetimes,
  chartType: "stepped",
  series: [{ label: "State", values: [0, 1, 1] }],
});
```

### Missing Data With Interpolation

```ts
await renderChartPng({
  datetimes,
  series: [{ label: "Wind", values: [10, null, 12] }],
  nullHandling: "interpolate",
});
```

### Axis Controls And Styling

```ts
await renderChartPng({
  datetimes,
  series: [{ label: "Temp", values: [37, 35, 36] }],
  xAxis: { label: "Time", tickRotation: 45, dateFormat: { format: "HH:mm" } },
  yAxis: { label: "F", min: 30, max: 40, tickStepSize: 2 },
  style: { fontFamily: "Arial", fontSize: 12, gridColor: "#e5e7eb", plotAreaColor: "#f8fafc" },
});
```

### Annotations

```ts
await renderChartPng({
  datetimes,
  series: [{ label: "Severity", values: [1, 2, 3] }],
  annotations: {
    thresholds: [{ y: 2.5, label: "Severe" }],
    markers: [{ x: datetimes[1], label: "Issued" }],
    ranges: [{ start: datetimes[0], end: datetimes[2], label: "Window" }],
  },
});
```
