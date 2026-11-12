# Validation scope

The source workflow completed a large four-product GEDI V3 download, source/geometry audits, and an exhaustively verified Parquet consolidation. The reusable package keeps that acquisition and QC logic while replacing study-specific paths with a user-supplied AOI and configurable products/attributes.

Offline integration tests exercise actual polygon-file ingestion, HDF5 datasets, a SQLite manifest, source-linked QC indexes, cross-product matching, native outlier flags, and a complete Parquet write/read verification. Cases cover the default four products, reduced/custom L2B-only acquisition, unknown orbit screens, overlapping AOI rejection and completed-run resume. Tests do not connect to NASA or require Earthdata credentials. Default attribute names are also checked against saved provider capability metadata from the source workflow.

A fresh generic-AOI bulk download has not been repeated during packaging. NASA credentials, authorization and current service availability must be confirmed by each researcher. GitHub Actions is configured for Windows and Linux; local checks were run on Windows with Python 3.12. Configured CI is not a claim that remote CI has already run.

A wheel build verifies package metadata and inclusion of JSON presets/policy. User inputs, download products, local environments, credentials and test outputs are excluded from the upload archive. No original study datasets are included or modified by packaging.

## Local release check — 2026-11-12

- Python wheel built successfully and installed into an isolated directory.
- All four offline integration tests passed against the installed wheel.
- The final shot schema includes generic AOI identifiers and GEDI fields only.
- Default attribute selections validated against saved NASA capabilities for all four products.
- Source files syntax checked; upload bundle checked for study-specific paths, credentials and binary datasets.
