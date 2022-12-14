"""Helper functions to preprocess data"""

from pathlib import Path 
import geopandas as gpd 
from shapely.geometry import box
from typing import Union
import shutil

BASE = Path(__file__).resolve().parent
GRID = BASE.joinpath('OS_BNG_10km.gpkg')

def iterate_grid_cells() -> list:
    """Returns bounding box of cell as list"""
    gdf = gpd.read_file(GRID, layer='OS_10km_tiles_buildings')
    #######TESTING ONLY######
    # gdf = gdf[gdf.tile_name.isin(['TL23', 'TL24', 'TL25', 'TL26', 'TL33', 'TL34', 'TL35', 'TL36', 'TL43', 'TL44', 'TL45', 'TL46', 'TL53', 'TL54', 'TL55', 'TL56', 'TL63', 'TL64', 'TL65', 'TL66'])]
    for row in gdf.itertuples():
        yield list(row.geometry.bounds), row.tile_name

def extract_from_buildings(
    gpkg: Union[Path, str],
    layer: str,
    bbox: list) -> gpd.GeoDataFrame:
    """Extracts gpkg layer in bbox"""
    gdf = gpd.read_file(gpkg, layer=layer, bbox=bbox)
    polygon = box(*bbox)
    gdf_clip = gdf.clip(polygon)
    return gdf_clip

def save_gpkg_to_folder(
    gdf: gpd.GeoDataFrame,
    out_parent: Union[Path, str],
    raster_dir: Union[Path, str],
    tile_name: str
    ):
    """Saves gdf to folder named tile name in out_parent and moves corresponding raster to same dir"""
    out_dir = Path(out_parent).joinpath(tile_name)
    if not out_dir.exists():
        out_dir.mkdir(parents=True)
    gdf.to_file(out_dir.joinpath(f'{tile_name}.gpkg'), layer='buildings_uk', index=False)
    raster = raster_dir.joinpath(f'DSM_DTM_{tile_name}_m100_10K_Tile.tif')
    if not out_dir.joinpath(raster.name).exists():
        if raster.exists():
            shutil.move(raster, out_dir)
