# Graphing Module Overview

The `nws-alerts-graphing` package renders weather trend line charts as PNG images.

Key capabilities:

- Accepts a shared datetime X-axis.
- Accepts one or two Y-value series.
- Generates a PNG `Buffer`.
- Provides data URI output for direct HTML email embedding.

Package location:

- `packages/nws-alerts-graphing`

Primary exports:

- `renderLineGraphPng`
- `renderLineGraphDataUri`
- `toInlinePngDataUri`
