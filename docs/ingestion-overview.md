# Weather Ingestion Modules Overview

This document summarizes the ingestion systems currently modeled after the `nwsAlerts` services. Each ingestion module is implemented as an npm package in `wx-modules/packages/*` and is designed to be side-effect free (no email, no database writes).

## Common Output Shape

All ingestion packages return a `IngestionResult` containing:

- `alerts`: Normalized alert objects with `nwsId`, `event`, `headline`, `description`, `geometry`, and timestamps.
- `raw`: Optional raw payloads fetched from upstream sources.
- `meta`: Optional metadata about the ingestion run (issue times, counts, URLs).
- `dedupeKeys`: Stable strings suitable for de-duplication.

The shared `Alert` shape is defined in `packages/nws-alerts-ingest-core`.

## Systems

### NWS Active Alerts (by point)

- Source: `https://api.weather.gov/alerts/active?point=LAT,LON`
- Format: JSON (GeoJSON-like feature collection)
- Inputs: `lat`, `lon`
- Output: One alert per feature with `nwsId` from `feature.id` and geometry from `feature.geometry`.
- Notes: Designed to align with `nwsAlerts/src/services/Nws.ts` behavior.

### SPC Mesoscale Discussions (MD)

- Source: `https://www.spc.noaa.gov/products/spcmdrss.xml`
- Format: RSS XML
- Inputs: `lat`, `lon`
- Output: Alerts derived from MD items with `LAT...LON` polygons in the description.
- Notes: Filters alerts by point-in-polygon; converts NWS-style lat/lon strings into polygons.

### SPC Convective Outlooks (Day 1–8)

- Sources:
  - Day 1–3: `https://www.spc.noaa.gov/products/outlook/archive/YYYY/day{N}otlk_YYYYMMDD_HHMM_{type}.lyr.geojson`
  - Day 4–8: `https://www.spc.noaa.gov/products/exper/day4-8/archive/YYYY/day{N}prob_YYYYMMDD.lyr.geojson`
  - HTML: Day 1–3 `day{N}otlk_YYYYMMDD_HHMM.html`, Day 4–8 `day4-8_YYYYMMDD.html`
- Format: GeoJSON + HTML
- Inputs: `lat`, `lon`, issue-time window (derived from `now`)
- Output: Alerts for each day where a matching outlook window is active and risk levels include the point.
- Notes: Issue-time windows follow the logic in `nwsAlerts/src/services/Spc.ts`. Risk levels are derived from SPC labels (e.g., `SLGT`, `MDT`, `HIGH`, `0.15`, `CIG1`, `CIG2`, `CIG3`; Day 3 legacy `SIGN` may still appear during transition).

### WPC Excessive Rainfall Outlook (Day 1–5)

- Sources:
  - GeoJSON: `https://www.wpc.ncep.noaa.gov/exper/eromap/geojson/Day{N}_Latest.geojson`
  - Maps: `https://www.wpc.ncep.noaa.gov/exper/eromap/cwamaps/TSA_Day{N}.png`
- Format: GeoJSON + PNG
- Inputs: `lat`, `lon`, issue-time window (derived from `now`)
- Output: Alerts when the point is inside any risk polygon for the current issuance window.
- Notes: Mirrors the issuance windows used in `nwsAlerts/src/services/Wpc.ts` without generating maps or sending emails.

### WPC Probabilistic Winter Precipitation Forecast (PWPF) Snow

- Source: `https://www.wpc.ncep.noaa.gov/pwpf/latest_kml_GE/` (KML/KMZ)
- Format: KML/KMZ
- Inputs: `lat`, `lon`
- Output: A summary alert when probability or percentile forecasts intersect the point.
- Notes: Checks the same period/threshold combinations as `nwsAlerts/src/services/WpcSnow.ts` (12h/24h/48h/72h thresholds and 50th percentile accumulations).
