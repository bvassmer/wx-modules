import { formatInTimeZone } from "date-fns-tz";
import { ChartJSNodeCanvas } from "chartjs-node-canvas";
import {
  BarElement,
  CategoryScale,
  Chart,
  type ChartConfiguration,
  type ChartDataset,
  Filler,
  Legend,
  LineElement,
  LinearScale,
  PointElement,
  Tooltip,
} from "chart.js";
import annotationPlugin from "chartjs-plugin-annotation";
import type {
  AnnotationOptions as ChartJsAnnotationOptions,
  AnnotationTypeRegistry,
} from "chartjs-plugin-annotation";

Chart.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  BarElement,
  Filler,
  Tooltip,
  Legend,
  annotationPlugin,
);

const DEFAULT_WIDTH = 900;
const DEFAULT_HEIGHT = 420;
const DEFAULT_BACKGROUND_COLOR = "#ffffff";
const DEFAULT_TIMEZONE = "UTC";
const DEFAULT_DATE_FORMAT = "MMM dd, HH:mm";
const DEFAULT_PALETTE = [
  "#2563eb",
  "#ef4444",
  "#10b981",
  "#f59e0b",
  "#8b5cf6",
  "#14b8a6",
];
const DEFAULT_LEGEND_MAX_LABEL_LENGTH = 24;

/** Date-compatible input for labels and annotations. */
export type DateValue = Date | string | number;

/** Supported chart rendering modes. */
export type ChartType = "line" | "bar" | "area" | "stepped";
/** How to treat null values in series data. */
export type NullHandling = "gap" | "interpolate";

/** A single data series rendered on the chart. */
export type GraphSeries = {
  /** Numeric values aligned to `datetimes`; `null` may represent missing data. */
  values: Array<number | null>;
  /** Legend label for the series. */
  label?: string;
  /** Stroke color for line/bar border (overrides palette). */
  lineColor?: string;
  /** Fill color for area/bar (defaults to line color). */
  fillColor?: string;
};

/** Axis configuration and ticks. */
export type AxisOptions = {
  /** Axis title text. */
  label?: string;
  /** Minimum axis bound (date-like for X axis, number for Y axis). */
  min?: number | DateValue;
  /** Maximum axis bound (date-like for X axis, number for Y axis). */
  max?: number | DateValue;
  /** Maximum number of tick labels to render. */
  tickMax?: number;
  /** Explicit numeric tick step size. */
  tickStepSize?: number;
  /** Maximum tick label rotation in degrees. */
  tickRotation?: number;
  /** Minimum tick label rotation in degrees. */
  tickRotationMin?: number;
};

/** Date formatting options using date-fns format tokens. */
export type DateFormatOptions = {
  /** date-fns format token string used for X-axis labels. */
  format: string;
  /** IANA timezone used when formatting labels (defaults to UTC). */
  timeZone?: string;
};

/** Style configuration for fonts, grid, and sizing. */
export type StyleOptions = {
  /** Font family used across chart labels and title. */
  fontFamily?: string;
  /** Base font size in pixels. */
  fontSize?: number;
  /** Foreground text color for labels and titles. */
  fontColor?: string;
  /** Gridline color for both axes. */
  gridColor?: string;
  /** Canvas background color. */
  backgroundColor?: string;
  /** Plot area background color (inside chart axes). */
  plotAreaColor?: string;
  /** Stroke width for line series and borders. */
  lineWidth?: number;
  /** Point radius for line/stepped charts. */
  pointRadius?: number;
};

/** Legend configuration (e.g., label truncation). */
export type LegendOptions = {
  /** Maximum label length before truncation. Set to 0 to disable. */
  maxLabelLength?: number;
};

/** Horizontal threshold line. */
export type AnnotationThreshold = {
  /** Y-axis value where the threshold line is drawn. */
  y: number;
  /** Optional label shown on the threshold line. */
  label?: string;
  /** Threshold line and label color. */
  color?: string;
  /** Threshold line width in pixels. */
  lineWidth?: number;
  /** Optional dash pattern used for the line. */
  dash?: number[];
};

/** Vertical marker line. */
export type AnnotationMarker = {
  /** X-axis date/time where the marker is drawn. */
  x: DateValue;
  /** Optional label shown on the marker. */
  label?: string;
  /** Marker line and label color. */
  color?: string;
  /** Marker line width in pixels. */
  lineWidth?: number;
  /** Optional dash pattern used for the marker line. */
  dash?: number[];
};

/** Shaded range for a period. */
export type AnnotationRange = {
  /** Start of the shaded range (inclusive). */
  start: DateValue;
  /** End of the shaded range (inclusive). */
  end: DateValue;
  /** Optional centered label for the range. */
  label?: string;
  /** Background color for the range box. */
  color?: string;
  /** Opacity applied when default range color is used. */
  opacity?: number;
};

/** Annotation configuration for thresholds, markers, and ranges. */
export type AnnotationOptions = {
  /** Horizontal threshold lines. */
  thresholds?: AnnotationThreshold[];
  /** Vertical point-in-time markers. */
  markers?: AnnotationMarker[];
  /** Shaded time windows. */
  ranges?: AnnotationRange[];
};

/** Full-featured chart render input. */
export type RenderChartInput = {
  /** X-axis datetime values shared by all series. */
  datetimes: DateValue[];
  /** One or more data series, each matching `datetimes.length`. */
  series: GraphSeries[];
  /** Rendering mode (defaults to `"line"`). */
  chartType?: ChartType;
  /** Whether area series should be stacked (`chartType: "area"` only). */
  areaStacked?: boolean;
  /** Null treatment strategy (defaults to `"gap"`). */
  nullHandling?: NullHandling;
  /** Color palette used when a series does not define custom colors. */
  palette?: string[];
  /** Legend behavior options. */
  legend?: LegendOptions;
  /** X-axis options and date formatting controls. */
  xAxis?: AxisOptions & { dateFormat?: DateFormatOptions };
  /** Y-axis options including optional baseline behavior. */
  yAxis?: AxisOptions & { beginAtZero?: boolean };
  /** Typography and visual styling options. */
  style?: StyleOptions;
  /** Chart title text. */
  title?: string;
  /** Output image width in pixels. */
  width?: number;
  /** Output image height in pixels. */
  height?: number;
  /** Optional threshold/marker/range overlays. */
  annotations?: AnnotationOptions;
  /** Top-level canvas background color override. */
  backgroundColor?: string;
};

/** Legacy line chart input, kept for backwards compatibility. */
export type RenderGraphInput = {
  /** X-axis datetime values shared by all series. */
  datetimes: DateValue[];
  /** One or more line series, each matching `datetimes.length`. */
  series: GraphSeries[];
  /** Optional chart title. */
  title?: string;
  /** Optional Y-axis title. */
  yAxisLabel?: string;
  /** Optional X-axis title. */
  xAxisLabel?: string;
  /** Output image width in pixels. */
  width?: number;
  /** Output image height in pixels. */
  height?: number;
  /** Top-level canvas background color override. */
  backgroundColor?: string;
};

const toDate = (value: DateValue): Date => {
  if (value instanceof Date) {
    return value;
  }

  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    throw new Error(`Invalid datetime value: ${String(value)}`);
  }

  return parsed;
};

const formatDateLabel = (
  value: DateValue,
  options?: DateFormatOptions,
): string => {
  const zone = options?.timeZone ?? DEFAULT_TIMEZONE;
  const format = options?.format ?? DEFAULT_DATE_FORMAT;

  return formatInTimeZone(toDate(value), zone, format);
};

const assertValidInput = (input: RenderChartInput) => {
  if (!input.datetimes.length) {
    throw new Error("datetimes must contain at least 1 value");
  }

  if (!input.series.length) {
    throw new Error("series must contain at least 1 value array");
  }

  for (const [index, item] of input.series.entries()) {
    if (item.values.length !== input.datetimes.length) {
      throw new Error(
        `series[${index}].values length (${item.values.length}) must match datetimes length (${input.datetimes.length})`,
      );
    }
  }
};

const interpolateSeries = (
  values: Array<number | null>,
): Array<number | null> => {
  const output = [...values];
  const length = output.length;

  const findNextValue = (start: number) => {
    for (let index = start; index < length; index += 1) {
      const value = output[index];
      if (value !== null) {
        return { index, value };
      }
    }
    return null;
  };

  let previousIndex = -1;
  let previousValue: number | null = null;

  for (let index = 0; index < length; index += 1) {
    const current = output[index];
    if (current !== null) {
      previousIndex = index;
      previousValue = current;
      continue;
    }

    const next = findNextValue(index + 1);
    if (!next || previousValue === null) {
      continue;
    }

    const gapLength = next.index - previousIndex;
    for (let gapIndex = 1; gapIndex < gapLength; gapIndex += 1) {
      const ratio = gapIndex / gapLength;
      output[previousIndex + gapIndex] =
        previousValue + (next.value - previousValue) * ratio;
    }

    index = next.index - 1;
  }

  return output;
};

const truncateLegendLabel = (label: string, maxLength: number | undefined) => {
  if (!maxLength || label.length <= maxLength) {
    return label;
  }

  if (maxLength <= 1) {
    return "...";
  }

  return `${label.slice(0, Math.max(0, maxLength - 3))}...`;
};

const buildPlotAreaPlugin = (plotAreaColor?: string) => {
  if (!plotAreaColor) {
    return undefined;
  }

  return {
    id: "plotAreaBackground",
    beforeDraw: (chart: Chart) => {
      const { ctx, chartArea } = chart;
      if (!chartArea) {
        return;
      }

      ctx.save();
      ctx.fillStyle = plotAreaColor;
      ctx.fillRect(
        chartArea.left,
        chartArea.top,
        chartArea.width,
        chartArea.height,
      );
      ctx.restore();
    },
  };
};

const buildAnnotations = (input: RenderChartInput, labels: string[]) => {
  const annotations = input.annotations;
  if (!annotations) {
    return undefined;
  }

  const items: Record<
    string,
    ChartJsAnnotationOptions<keyof AnnotationTypeRegistry>
  > = {};
  const formatOptions = input.xAxis?.dateFormat;

  const labelFor = (value: DateValue) => formatDateLabel(value, formatOptions);

  annotations.thresholds?.forEach((threshold, index) => {
    items[`threshold-${index}`] = {
      type: "line",
      yMin: threshold.y,
      yMax: threshold.y,
      borderColor: threshold.color ?? "#dc2626",
      borderWidth: threshold.lineWidth ?? 1.5,
      borderDash: threshold.dash,
      label: threshold.label
        ? {
            display: true,
            content: threshold.label,
            position: "end",
            color: threshold.color ?? "#dc2626",
          }
        : undefined,
    };
  });

  annotations.markers?.forEach((marker, index) => {
    const markerLabel = labelFor(marker.x);
    if (!labels.includes(markerLabel)) {
      return;
    }

    items[`marker-${index}`] = {
      type: "line",
      xMin: markerLabel,
      xMax: markerLabel,
      borderColor: marker.color ?? "#475569",
      borderWidth: marker.lineWidth ?? 1.5,
      borderDash: marker.dash,
      label: marker.label
        ? {
            display: true,
            content: marker.label,
            position: "start",
            color: marker.color ?? "#475569",
          }
        : undefined,
    };
  });

  annotations.ranges?.forEach((range, index) => {
    const startLabel = labelFor(range.start);
    const endLabel = labelFor(range.end);
    if (!labels.includes(startLabel) || !labels.includes(endLabel)) {
      return;
    }

    const opacity = range.opacity ?? 0.15;
    items[`range-${index}`] = {
      type: "box",
      xMin: startLabel,
      xMax: endLabel,
      backgroundColor: range.color ?? `rgba(148, 163, 184, ${opacity})`,
      borderWidth: 0,
      label: range.label
        ? {
            display: true,
            content: range.label,
            position: "center",
            color: "#1f2937",
          }
        : undefined,
    };
  });

  return items;
};

/** Render a chart as a PNG buffer. */
export const renderChartPng = async (
  input: RenderChartInput,
): Promise<Buffer> => {
  assertValidInput(input);

  const width = input.width ?? DEFAULT_WIDTH;
  const height = input.height ?? DEFAULT_HEIGHT;
  const chartType = input.chartType ?? "line";
  const palette = input.palette ?? DEFAULT_PALETTE;
  const nullHandling = input.nullHandling ?? "gap";
  const legendMax = input.legend?.maxLabelLength;
  const legendMaxResolved =
    legendMax === undefined ? DEFAULT_LEGEND_MAX_LABEL_LENGTH : legendMax;
  const fontFamily = input.style?.fontFamily;
  const fontSize = input.style?.fontSize;
  const fontColor = input.style?.fontColor ?? "#0f172a";
  const gridColor = input.style?.gridColor ?? "#e2e8f0";
  const lineWidth = input.style?.lineWidth ?? 2;
  const pointRadius = input.style?.pointRadius ?? 2;
  const chartBackground =
    input.backgroundColor ??
    input.style?.backgroundColor ??
    DEFAULT_BACKGROUND_COLOR;
  const plotAreaPlugin = buildPlotAreaPlugin(input.style?.plotAreaColor);

  const labels = input.datetimes.map((value) =>
    formatDateLabel(value, input.xAxis?.dateFormat),
  );

  type SeriesValues = Array<number | null>;
  type SupportedDataset = ChartDataset<"line" | "bar", SeriesValues>;

  const datasets: SupportedDataset[] = input.series.map((series, index) => {
    const paletteColor =
      palette[index % palette.length] ??
      DEFAULT_PALETTE[index % DEFAULT_PALETTE.length];
    const stroke = series.lineColor ?? paletteColor;
    const fillColor = series.fillColor ?? stroke;
    const values =
      nullHandling === "interpolate"
        ? interpolateSeries(series.values)
        : series.values;

    const baseDataset = {
      label: series.label ?? `Series ${index + 1}`,
      data: values,
      borderColor: stroke,
      backgroundColor: fillColor,
      borderWidth: lineWidth,
    };

    if (chartType === "bar") {
      return {
        ...baseDataset,
        type: "bar",
      } as SupportedDataset;
    }

    return {
      ...baseDataset,
      type: "line",
      tension: chartType === "stepped" ? 0 : 0.2,
      stepped: chartType === "stepped" ? true : undefined,
      pointRadius,
      spanGaps: nullHandling === "gap" ? false : undefined,
      fill: chartType === "area" ? "origin" : false,
    } as SupportedDataset;
  });

  const chartCanvas = new ChartJSNodeCanvas({
    width,
    height,
    backgroundColour: chartBackground,
  });

  const annotations = buildAnnotations(input, labels);

  const config: ChartConfiguration<"line" | "bar", SeriesValues, string> = {
    type: chartType === "bar" ? "bar" : "line",
    data: {
      labels,
      datasets,
    },
    options: {
      responsive: false,
      plugins: {
        legend: {
          display: true,
          position: "top",
          labels: {
            color: fontColor,
            font:
              fontFamily || fontSize
                ? { family: fontFamily, size: fontSize }
                : undefined,
            generateLabels: (chart) => {
              const base =
                Chart.defaults.plugins.legend.labels.generateLabels(chart);
              return base.map((item) => ({
                ...item,
                text: truncateLegendLabel(item.text ?? "", legendMaxResolved),
              }));
            },
          },
        },
        title: {
          display: Boolean(input.title),
          text: input.title,
          color: fontColor,
          font:
            fontFamily || fontSize
              ? { family: fontFamily, size: fontSize }
              : undefined,
        },
        annotation: annotations ? { annotations } : undefined,
      },
      scales: {
        x: {
          stacked:
            chartType === "area" ? Boolean(input.areaStacked) : undefined,
          min: input.xAxis?.min
            ? formatDateLabel(
                input.xAxis.min as DateValue,
                input.xAxis?.dateFormat,
              )
            : undefined,
          max: input.xAxis?.max
            ? formatDateLabel(
                input.xAxis.max as DateValue,
                input.xAxis?.dateFormat,
              )
            : undefined,
          ticks: {
            color: fontColor,
            maxTicksLimit: input.xAxis?.tickMax,
            minRotation: input.xAxis?.tickRotationMin,
            maxRotation: input.xAxis?.tickRotation,
            font:
              fontFamily || fontSize
                ? { family: fontFamily, size: fontSize }
                : undefined,
          },
          grid: {
            color: gridColor,
          },
          title: {
            display: Boolean(input.xAxis?.label),
            text: input.xAxis?.label,
            color: fontColor,
            font:
              fontFamily || fontSize
                ? { family: fontFamily, size: fontSize }
                : undefined,
          },
        },
        y: {
          stacked:
            chartType === "area" ? Boolean(input.areaStacked) : undefined,
          beginAtZero: input.yAxis?.beginAtZero ?? false,
          min:
            typeof input.yAxis?.min === "number" ? input.yAxis?.min : undefined,
          max:
            typeof input.yAxis?.max === "number" ? input.yAxis?.max : undefined,
          ticks: {
            color: fontColor,
            maxTicksLimit: input.yAxis?.tickMax,
            stepSize: input.yAxis?.tickStepSize,
            minRotation: input.yAxis?.tickRotationMin,
            maxRotation: input.yAxis?.tickRotation,
            font:
              fontFamily || fontSize
                ? { family: fontFamily, size: fontSize }
                : undefined,
          },
          grid: {
            color: gridColor,
          },
          title: {
            display: Boolean(input.yAxis?.label),
            text: input.yAxis?.label,
            color: fontColor,
            font:
              fontFamily || fontSize
                ? { family: fontFamily, size: fontSize }
                : undefined,
          },
        },
      },
    },
    plugins: plotAreaPlugin ? [plotAreaPlugin] : [],
  };

  return chartCanvas.renderToBuffer(config, "image/png");
};

/** Render a chart as a data URI string. */
export const renderChartDataUri = async (
  input: RenderChartInput,
): Promise<string> => {
  const png = await renderChartPng(input);
  return toInlinePngDataUri(png);
};

/** Legacy line chart PNG renderer. */
export const renderLineGraphPng = async (
  input: RenderGraphInput,
): Promise<Buffer> => {
  const chartInput: RenderChartInput = {
    datetimes: input.datetimes,
    series: input.series,
    title: input.title,
    width: input.width,
    height: input.height,
    backgroundColor: input.backgroundColor,
    chartType: "line",
    xAxis: input.xAxisLabel
      ? {
          label: input.xAxisLabel,
        }
      : undefined,
    yAxis: input.yAxisLabel
      ? {
          label: input.yAxisLabel,
        }
      : undefined,
  };

  return renderChartPng(chartInput);
};

/** Convert a PNG buffer to a data URI string. */
export const toInlinePngDataUri = (pngBuffer: Buffer): string => {
  return `data:image/png;base64,${pngBuffer.toString("base64")}`;
};

/** Legacy line chart data URI renderer. */
export const renderLineGraphDataUri = async (
  input: RenderGraphInput,
): Promise<string> => {
  const png = await renderLineGraphPng(input);
  return toInlinePngDataUri(png);
};
