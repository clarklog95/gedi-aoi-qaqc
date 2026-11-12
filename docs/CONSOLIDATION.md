# Verified consolidation

Run `python run_gedi.py combine` after QA/QC. The package selects the union of shots with at least one selected-product `qc_usable` decision, orders by uint64 shot ID, and assembles source-native values in configurable batches (default 100,000). Missing products have `present=false`, false usability masks, and null measurements. Present-but-failed values remain with their own flags.

Each staging batch is written with Zstandard compression, reread, and checked for all scalar values, profile elements, null masks, schema, and shot IDs. An atomic manifest checkpoint stores its checksum, counts and input fingerprint. Final assembly streams row groups into one Parquet file, then compares every row group against verified staging batches and checks the complete sorted ID union and product QC counts. A completed file is published by rename only after validation. The report records its SHA256 checksum.

Verified staging batches are retained for resumability. A second copy of compressed data therefore remains on disk. Cleanup is a separate user decision after verification and backup; the workflow does not delete source datasets. Completed-run resume validates the frozen input fingerprint; keep final reports and backups. Do not change source files or configuration inside an existing project.

Input indexes and QC decisions live under `qa_steps1_5/`; native measurement fields come from `data/full_download/`. Units, dimensions, native attributes, QC policy and source catalog are embedded in schema metadata. The main file is not GeoParquet: shot coordinates and AOI IDs are columns, while authoritative polygon geometry remains in `data/study_area/aoi_units.gpkg`.
