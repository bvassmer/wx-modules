# wx-modules Agent Guide

## Purpose

- `wx-modules` contains shared weather packages used by other repos in this workspace.
- It is not deployed as a standalone service. Its code ships through downstream consumers such as `nwsAlerts` or other weather-stack apps.

## Deployment Routing

- If a package change affects ingestion behavior, redeploy the consumer on `nws`, typically `nwsAlerts`.
- If a package change affects UI or API behavior, rebuild and redeploy the corresponding service from `weather-llm-iac`.
- `wx-modules` changes still ship through the consumer repo's Git-based Pi deploy flow: push the package change and the consumer repo change to GitHub, then redeploy the consumer from its live Git checkout on the target Pi.
- Do not copy built package files directly onto a Pi; use the consumer repo's deploy wrapper so the runtime checkout stays Git-backed and reproducible.

## Validation

- Validate the downstream app, not just the package build.
- Keep package changes tied to the repo that consumes them so the Pi deploy includes the updated package output.

## References

- See `docs/ingestion-overview.md` and `docs/graphing-overview.md` for package roles.
- See `../weather-llm-iac/AGENTS.md` for the live two-Pi deployment layout.
