# nws-alerts-forecast-summary-ollama

Creates a one-sentence forecast trend summary for a selected forecast variable by calling an Ollama model.

## Install

This package is part of the `wx-modules` workspace.

## Usage

```ts
import {
  ForecastVariable,
  summarizeForecastVariable,
} from "nws-alerts-forecast-summary-ollama";

const summary = await summarizeForecastVariable({
  variable: ForecastVariable.Temperature,
  points: [
    { date: "2026-02-15T10:00:00Z", value: 42 },
    { date: "2026-02-15T11:00:00Z", value: 45 },
    { date: "2026-02-15T12:00:00Z", value: 47 },
  ],
  unit: "F",
  model: "llama3.1",
});
```

The package calls `POST /api/generate` on Ollama (default host `http://localhost:11434`) and enforces a one-sentence result.
