/**
 * Supported forecast variables that can be summarized.
 */
export enum ForecastVariable {
  Temperature = "temperature",
  Dewpoint = "dewpoint",
  RelativeHumidity = "relativeHumidity",
  ApparentTemperature = "apparentTemperature",
  WindSpeed = "windSpeed",
  WindGust = "windGust",
  SkyCover = "skyCover",
  ProbabilityOfPrecipitation = "probabilityOfPrecipitation",
  QuantitativePrecipitation = "quantitativePrecipitation",
  SnowfallAmount = "snowfallAmount",
  IceAccumulation = "iceAccumulation",
}

/**
 * A single dated forecast value sample.
 */
export type ForecastValuePoint = {
  /** Datetime for the sample, as a `Date` or parseable string. */
  date: string | Date;
  /** Numeric value or `null` when the value is unavailable. */
  value: number | null;
};

/**
 * Input payload for generating a one-sentence forecast summary.
 */
export type SummarizeForecastRequest = {
  /** Forecast variable represented by the supplied points. */
  variable: ForecastVariable;
  /** Time-ordered or unsorted forecast points to summarize. */
  points: ForecastValuePoint[];
  /** Optional unit label (for example `F`, `%`, `mph`) used in prompt context. */
  unit?: string;
  /** Optional Ollama model override for this request. */
  model?: string;
  /** Optional Ollama host override for this request. */
  host?: string;
};

/**
 * Shared client options for Ollama forecast summarization requests.
 */
export type ForecastSummaryClientOptions = {
  /** Default Ollama host URL when not specified on a request. */
  host?: string;
  /** Default Ollama model when not specified on a request. */
  model?: string;
  /** Timeout in milliseconds for a single model request. */
  requestTimeoutMs?: number;
  /** Optional fetch implementation override. */
  fetchImpl?: typeof fetch;
};

type OllamaGenerateResponse = {
  response?: string;
};

const DEFAULT_OLLAMA_HOST = "http://localhost:11434";
const DEFAULT_OLLAMA_MODEL = "llama3.1";
const DEFAULT_REQUEST_TIMEOUT_MS = 15000;

/**
 * Converts a user-provided date value into an ISO-8601 timestamp string.
 *
 * @param value Date value as a `Date` instance or parseable string.
 * @returns ISO timestamp string.
 * @throws If the value cannot be parsed as a valid date.
 */
const toIsoDate = (value: string | Date): string => {
  if (value instanceof Date) {
    return value.toISOString();
  }

  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    throw new Error(`Invalid date value: ${value}`);
  }

  return date.toISOString();
};

/**
 * Validates, normalizes, and sorts forecast points in chronological order.
 *
 * @param points Raw forecast points from the request.
 * @returns Points with ISO date strings sorted oldest to newest.
 * @throws If no points are provided or any value is not finite/null.
 */
const normalizePoints = (points: ForecastValuePoint[]) => {
  if (!Array.isArray(points) || points.length === 0) {
    throw new Error("points must include at least one date/value pair");
  }

  const normalized = points.map((point) => {
    const value = point.value;
    if (value !== null && !Number.isFinite(value)) {
      throw new Error("point value must be a finite number or null");
    }

    return {
      date: toIsoDate(point.date),
      value,
    };
  });

  return normalized.sort((left, right) => {
    return Date.parse(left.date) - Date.parse(right.date);
  });
};

/**
 * Reduces model output to exactly one sentence.
 *
 * @param text Raw response text returned from Ollama.
 * @returns A single-sentence summary ending in terminal punctuation.
 * @throws If the model response is empty after trimming.
 */
const toSingleSentence = (text: string): string => {
  const trimmed = text.trim().replace(/\s+/g, " ");
  if (!trimmed) {
    throw new Error("Ollama returned an empty summary");
  }

  const firstSentenceMatch = trimmed.match(/^[^.!?]+[.!?]/);
  if (firstSentenceMatch) {
    return firstSentenceMatch[0].trim();
  }

  return `${trimmed}.`;
};

/**
 * Builds the deterministic prompt used to request a one-sentence forecast summary.
 *
 * @param variable Forecast variable being summarized.
 * @param points Normalized chronological data points.
 * @param unit Optional display unit for the variable values.
 * @returns Prompt text to send to Ollama.
 */
const buildPrompt = (
  variable: ForecastVariable,
  points: Array<{ date: string; value: number | null }>,
  unit?: string,
): string => {
  return [
    "You are summarizing weather forecast data.",
    "Return exactly one sentence and no extra text.",
    `Forecast variable: ${variable}`,
    `Unit: ${unit ?? "not provided"}`,
    `Data points (chronological JSON): ${JSON.stringify(points)}`,
    "Describe the overall trend and notable change direction across the timeframe.",
  ].join("\n");
};

/**
 * Generates a single-sentence trend summary for one forecast variable by calling Ollama.
 *
 * @param request Forecast variable, data points, and optional request overrides.
 * @param options Client-level defaults and transport configuration.
 * @returns A single sentence describing overall trend and direction.
 * @throws If request validation fails, Ollama returns an invalid payload,
 * or the request times out.
 */
export const summarizeForecastVariable = async (
  request: SummarizeForecastRequest,
  options: ForecastSummaryClientOptions = {},
): Promise<string> => {
  const points = normalizePoints(request.points);
  const host = request.host ?? options.host ?? DEFAULT_OLLAMA_HOST;
  const model = request.model ?? options.model ?? DEFAULT_OLLAMA_MODEL;
  const fetchImpl = options.fetchImpl ?? globalThis.fetch;

  if (typeof fetchImpl !== "function") {
    throw new Error("No fetch implementation available to call Ollama");
  }

  const controller = new AbortController();
  const timeoutMs = options.requestTimeoutMs ?? DEFAULT_REQUEST_TIMEOUT_MS;
  const timeoutHandle = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const response = await fetchImpl(`${host}/api/generate`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      signal: controller.signal,
      body: JSON.stringify({
        model,
        stream: false,
        prompt: buildPrompt(request.variable, points, request.unit),
      }),
    });

    if (!response.ok) {
      throw new Error(`Ollama request failed with status ${response.status}`);
    }

    const payload = (await response.json()) as OllamaGenerateResponse;
    if (!payload.response || typeof payload.response !== "string") {
      throw new Error(
        "Ollama response payload did not include a valid summary",
      );
    }

    return toSingleSentence(payload.response);
  } catch (error) {
    if (error instanceof Error && error.name === "AbortError") {
      throw new Error("Timed out waiting for Ollama response");
    }

    throw error;
  } finally {
    clearTimeout(timeoutHandle);
  }
};
