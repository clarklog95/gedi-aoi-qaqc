# GEDI AOI download, QA/QC and Parquet workflow

Download selected GEDI Version 3 attributes for polygon areas of interest, retain source-linked quality decisions, and write one verified shot-level Parquet file. Researchers supply a regional polygon AOI, choose GEDI products and attributes, authenticate with Earthdata, and obtain analysis-ready shot records with explicit quality flags.

**A free NASA Earthdata Login account is required for downloading.** Create or manage your account at <https://urs.earthdata.nasa.gov/>. A browser is useful for account setup and token generation; bulk downloading and processing run in Python.

## 1. Install

Use Python 3.11 or newer (tested locally with Python 3.12 on Windows). From this repository:

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install -e .
```

On macOS/Linux activate with `source .venv/bin/activate`. NumPy, h5py, Shapely, pyogrio/GDAL, pyproj, PyArrow and DuckDB are installed as dependencies. Tkinter is optional for separate progress windows; install your platform's Tk package if needed. Processing itself does not require Tkinter, R, a GIS desktop application, or a cloud storage account.

## 2. Edit the settings at the top of `run_gedi.py`

```python
AOI_FILE = Path("inputs/my_aoi.gpkg")
AOI_LAYER = None
OUTPUT_DIRECTORY = Path("outputs/my_gedi_project")
PRODUCTS = ["L2A", "L2B", "L4A", "L4C"]
ATTRIBUTES = {product: list(paths) for product, paths in DEFAULT_ATTRIBUTES.items()}
START_DATE = "2019-04-18T00:00:00Z"
END_DATE = "2026-11-11T23:59:59Z"
```

The default products, forest attribute preset, QC policy, and date cutoff reproduce the choices used in the original study workflow. **The AOI must be supplied by you; study data and credentials are not distributed.** The end date is deliberately fixed for reproducibility; change it to request later acquisitions. Missing periods and product availability are determined by NASA inventory, not filled or interpolated.

Supported polygon inputs include GeoPackage, shapefile and GeoJSON, with a declared CRS. Supply a layer name for a multi-layer file. Keep shapefile components together. Features must be valid, nonempty Polygon/MultiPolygon geometries with nonoverlapping interiors. Each feature becomes an analysis unit: a single AOI feature produces one unit; input analysis polygons remain individual units. Boundary ties go to the first input feature. Regional AOIs only; split dateline-crossing or very large AOIs. The workflow does not create a grid or resample analysis polygons.

Optional mappings at the top of the script:

| Setting | Meaning | Default |
|---|---|---|
| `CELL_ID_FIELD` | Unique polygon ID | Generated IDs in feature order |
| `DOMAIN_ID_FIELD` | Optional group of AOI features | One domain called AOI |

AOI identifiers appear as `aoi_id` and `domain_id` in the output. Other input attributes are not copied into GEDI shot records. Polygon areas are calculated in EPSG:6933, an equal-area CRS.

### Choose products and attributes

Remove unwanted products from `PRODUCTS`, for example `PRODUCTS = ["L2A", "L2B"]`. Edit lists in `ATTRIBUTES` to reduce science fields:

```python
ATTRIBUTES["L2A"] = ["rh"]
ATTRIBUTES["L2B"] = ["cover", "pai", "pavd_z", "fhd_normal"]
ATTRIBUTES["L4A"] = ["agbd", "agbd_se"]
ATTRIBUTES["L4C"] = ["wsci", "wsci_xy", "wsci_z"]
```

Use native beam-relative HDF5 paths such as `rh` or `land_cover_data/pft_class`, without `/BEAM0000/`. Complete default lists are in [presets.json](src/gedi_aoi/presets.json). `python run_gedi.py capabilities` downloads provider metadata and validates requested names before data requests. Unsupported names stop the workflow rather than being silently ignored. Other advertised scalar and one-dimensional profile attributes can be requested; unusual higher-rank arrays or non-scalar ancillary structures require extending the assembler and will fail explicitly.

The workflow automatically keeps a minimum set of matching, positioning, land-cover and product-quality fields. It also retains the metrics used by the original QC policy (L2A RH; L2B cover/PAI/FHD; L4A AGBD; L4C WSCI). Removing them from an attribute list does not remove them from acquisition, because QC depends on them. Thus this is selective downloading, with small amounts of necessary supporting data retained. L2B native time uses `geolocation/delta_time`.

Default forest measurements include:

| Product | Default forest structure attributes |
|---|---|
| L2A | All 101 RH percentiles, highest/lowest return elevations, waveform energy, number of modes |
| L2B | Canopy cover and vertical cover profile, PAI and vertical PAI/PAVD profiles, foliage height diversity, relative canopy heights, gap probability/error, clumping and reflectance-related attributes |
| L4A | Aboveground biomass density, standard error, prediction interval, extrapolation flags |
| L4C | WSCI, horizontal and vertical components, and prediction intervals |

Native units, data types, fill values and array dimensions are retained in the Parquet metadata. In particular, L2A RH is in meters and L2B RCH is stored in native centimeters. Lower RH percentiles can be negative; the original QC policy does not remove shots solely for that reason.

## 3. Authenticate

Either run interactively and enter your Earthdata username and password when prompted, or use an Earthdata user token. The password prompt is hidden. The script requests a token over HTTPS and holds it in memory; it does not write credentials into project files.

To create a token, log into Earthdata Login and use its user-token page. Follow NASA's [user token instructions](https://urs.earthdata.nasa.gov/documentation/for_users/user_token). To avoid displaying a token in PowerShell command history:

```powershell
$secureToken = Read-Host "Earthdata user token" -AsSecureString
$env:EARTHDATA_TOKEN = [System.Net.NetworkCredential]::new("", $secureToken).Password
python run_gedi.py all
Remove-Item Env:EARTHDATA_TOKEN
```

On macOS/Linux use `read -s EARTHDATA_TOKEN; export EARTHDATA_TOKEN`, then run the script and `unset EARTHDATA_TOKEN` afterward. Do not put credentials or tokens in Python settings, Git, screenshots, issue reports, or committed `.env` files. For authentication failures, verify the account/token directly with Earthdata Login and check any application authorization prompts from NASA. An expired token can be replaced before resuming. Local preparation, QA/QC, consolidation and progress viewing do not require an active login.

## 4. Run

```powershell
python run_gedi.py all
```

Or run stages separately:

```powershell
python run_gedi.py prepare
python run_gedi.py capabilities
python run_gedi.py download
python run_gedi.py qaqc
python run_gedi.py combine
```

In a second terminal, use `python run_gedi.py progress` while downloading, or `python run_gedi.py combine-progress` while combining. Closing a progress window does not stop its worker. Console logs also report completed sources/batches. Keep the worker terminal open to continue processing.

The downloader uses NASA CMR inventory, variable/spatial subsetting through Harmony for L2, and coordinate-guided OPeNDAP requests for L4. Bounding boxes find candidate data; exact polygon clipping occurs locally. L4 coordinate arrays and short coalesced runs can include rows outside the AOI during retrieval; these are removed before committed source subsets are written. Full-granule fallback is disabled. This reduces unnecessary transfer but does not promise zero redundant bytes.

Rerun the same command with the same output directory/settings to resume. Download manifest entries and completed Parquet staging batches are checkpoints. In-flight requests/batches may need to be repeated after disconnection or termination. The download progress window can pause dispatch/requests; combine pauses at batch/row-group checkpoints. To move a laptop, pause and let current requests settle, or stop the worker and resume afterward. Do not run two workers against the same project.

Settings are frozen in `project.json`. Use a new output directory when changing AOI, dates, products or attributes. A request with unresolved download jobs stops before QA/QC. Failures are retained in `metadata/full_download/manifest.sqlite`; inspect them before resetting attempts or treating a source as empty. Only a provider-confirmed empty spatial result is automatically accepted as empty.

## 5. Outputs and analysis

Final data: `outputs/my_gedi_project/data/combined/GEDI_all_products_QC_shots.parquet`.

One row represents one unique shot that passed QC for at least one selected product. Products join on uint64 `shot_number`; a missing product remains missing. Common columns include acquisition UTC time, AOI ID and domain ID. Product columns use prefixes such as `l2a__rh` and `l4a__agbd`. Array-valued profiles remain arrays, not 101 separately named scalar columns. Exact column count depends on chosen products, attributes, and provider schemas; no fixed column count is required.

**Filter each measurement by its own product usability flag**, e.g. `l4a__qc__qc_usable`, before analysis. A row can qualify through L2 while its available L4 values fail QC. Such native values are retained with their failed flags, not erased. Native numeric fill values are also preserved; Parquet nulls represent absent products/fields. Nighttime, leaf-on, sensitivity >=0.95 and complete-orbit-screen masks are available as sensitivity subsets. Missing optional supporting fields cannot establish membership in those subsets.

Orbit outliers are excluded if either available L4 product flags the shot. Without a matched L4 screen, an otherwise good L2 shot is retained and orbit-screen availability remains explicitly unknown. Choosing only L2 therefore changes orbit-screen availability relative to choosing all four products. All shots within the configured download date window are eligible for the quality checks. No forest means or statistical relationships are calculated.

Other outputs include source subsets in `data/full_download/`, the resumable SQLite manifest, QA indexes/reports in `qa_steps1_5/`, product-specific decisions, source/attribute metadata, verified combination batches, and final validation reports/checksums. Source subsets are retained; this workflow does not delete earlier datasets. Allow space for sources, indexes, temporary files, staging batches and the final Parquet together. Storage and time depend strongly on AOI/date range and NASA queues.

See [methods](docs/METHODS.md), [consolidation](docs/CONSOLIDATION.md), and [validation](docs/VALIDATION.md).

## Tests and development

```powershell
python -m unittest discover -s tests -v
```

Tests use synthetic local AOIs and HDF5 products, require no Earthdata credentials, and make no NASA requests. They exercise four-product processing, custom L2-only selection, absent orbit screens, rejected overlaps, final value validation, and completed-run resume. GitHub Actions runs them on Windows and Linux. Live NASA download services can change; inspect capabilities and manifest failures for every new run. The original study's large download was validated separately; a fresh generic-AOI bulk download was not repeated for packaging.

## Preparing an upload

Upload the contents of this repository folder, not its parent study project. AOI inputs, outputs, virtual environments and credential files are ignored by Git. No study HDF5, Parquet, QS or spatial files are bundled. Before adding a license, choose the terms under which you wish to distribute your code. Cite the NASA datasets and methods appropriate to your analysis; consult the product guides below for scientific interpretation.

## NASA documentation

- [Earthdata Login and tokens](https://urs.earthdata.nasa.gov/documentation/for_users/user_token)
- [Harmony API and capabilities](https://harmony.earthdata.nasa.gov/docs)
- [GEDI L2 Version 3 user guide](https://lpdaac.usgs.gov/documents/2442/GEDI02_User_Guide_V3.pdf)
- [GEDI L4A Version 3 guide](https://daac.ornl.gov/GEDI/guides/GEDI_L4A_AGB_Density_V3.html)
- [GEDI L4C Version 3 guide](https://daac.ornl.gov/GEDI/guides/GEDI_L4C_WSCI_V3.html)
