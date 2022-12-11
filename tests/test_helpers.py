"""Unittests for helpers.py"""

from pathlib import Path 
import pytest 
import geopandas as gpd
from shapely.geometry import box

import building_zonals as bz

#This will need to be set on other computers
GPKG = Path(r'C:\Users\dkerr\Documents\GISRede\buildings\UK\London\data\building_heights_tiles\buildings.gpkg').resolve()

@pytest.fixture
def bbox():
    grids = bz.iterate_grid_cells()
    bbox, tile_name = next(grids)
    yield bbox, tile_name


def test_iterate_grid_cells(bbox):    
    assert isinstance(bbox[0], list)
    assert bbox[1] == 'NT60'

def test_extract_bbox_from_gpkg(bbox):
    gdf_clip = bz.extract_from_buildings(GPKG, 'buildings_uk', bbox[0])
    bbox_got = list(gdf_clip.total_bounds)
    polygon_cell = box(*bbox[0])
    polygon_buildings = box(*bbox_got)
    assert len(gdf_clip.tile_name.unique()) == 1
    assert polygon_cell.contains(polygon_buildings)
    assert polygon_buildings.within(polygon_cell)
    assert isinstance(gdf_clip, gpd.GeoDataFrame)

def test_save_gpkg_to_folder(bbox):
    tile_name = bbox[1]
    box = bbox[0]
    gdf_clip = bz.extract_from_buildings(GPKG, 'buildings_uk', box)
    #This will need to be set on other computers
    out_parent = Path(r'C:\Users\dkerr\Documents\GISRede\buildings\UK\London\data\building_heights_tiles\rasters\tiles').resolve()
    raster_dir = out_parent.parent
    bz.save_gpkg_to_folder(gdf_clip, out_parent, raster_dir, tile_name)
    assert out_parent.joinpath('NT60/NT60.gpkg').exists()
    assert out_parent.joinpath('NT60/DSM_DTM_NT60_m100_10K_Tile.tif').exists()
