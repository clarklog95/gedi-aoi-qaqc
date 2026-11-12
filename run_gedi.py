"""Edit the settings below, then run: python run_gedi.py all"""
from pathlib import Path
from gedi_aoi.selection import DEFAULT_ATTRIBUTES, COLLECTIONS

# -------------------- USER SETTINGS --------------------
AOI_FILE = Path("inputs/my_aoi.gpkg")  # .gpkg, .shp, .geojson; Polygon/MultiPolygon, declared CRS
AOI_LAYER = None                       # Set layer name for a multi-layer spatial file
OUTPUT_DIRECTORY = Path("outputs/my_gedi_project")
PRODUCTS = ["L2A", "L2B", "L4A", "L4C"]

# Default = all forest metrics and supporting attributes used in the original workflow.
# Edit each list to keep only your desired native beam-relative attributes.
# Required positioning, matching and QC fields are added automatically.
ATTRIBUTES = {product: list(paths) for product, paths in DEFAULT_ATTRIBUTES.items()}
# Example: ATTRIBUTES["L2A"] = ["rh", "elev_highestreturn", "elev_lowestmode"]
# Example: ATTRIBUTES["L4A"] = ["agbd", "agbd_se"]
# Example: ATTRIBUTES["L4C"] = ["wsci", "wsci_xy", "wsci_z"]
# Example: ATTRIBUTES["L2B"] = ["cover", "pai", "pavd_z", "fhd_normal"]

START_DATE = "2019-04-18T00:00:00Z"
END_DATE = "2026-11-11T23:59:59Z"        # Original study cutoff; edit to extend the date range
WORKERS = 4
BATCH_SIZE = 100_000
MIN_FREE_GIB = 5

# Optional AOI identifiers. None generates unit IDs and one domain named AOI.
CELL_ID_FIELD = None
DOMAIN_ID_FIELD = None
# ------------------ END USER SETTINGS ------------------

def configuration():
    base=Path(__file__).resolve().parent
    return dict(aoi_file=str((base/AOI_FILE).resolve()),aoi_layer=AOI_LAYER,
        project_root=str((base/OUTPUT_DIRECTORY).resolve()),products=PRODUCTS,
        collections=COLLECTIONS,attributes=ATTRIBUTES,start=START_DATE,end=END_DATE,
        workers=WORKERS,batch_size=BATCH_SIZE,min_free_gib=MIN_FREE_GIB,
        aoi_fields=dict(cell_id=CELL_ID_FIELD,domain_id=DOMAIN_ID_FIELD))

if __name__=="__main__":
    from gedi_aoi.cli import execute
    execute(configuration())
