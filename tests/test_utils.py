"""Unit tests for utils.py"""

from pathlib import Path
import pytest

import geopandas as gpd
import pandas as pd
import rasterio

import building_zonals

DATA_DIR = Path(__file__).resolve().parent.joinpath('data')
SHP = DATA_DIR.joinpath('buildings_uk.shp')
RASTER_1 = DATA_DIR.joinpath('rasters/TQ38.tif')
RASTER_2 = DATA_DIR.joinpath('rasters/TQ38_2.tif')

OUT_GPKG = DATA_DIR.joinpath('buildings_uk.gpkg')
OUT_LAYER = 'buildings_uk'
ZONALS_CSV = DATA_DIR.joinpath('TQ38.csv')

#fixtures
@pytest.fixture
def gdf_clip():
    gdf = gpd.read_file(OUT_GPKG, layer=OUT_LAYER)
    gdf_clip = building_zonals.get_buildings_using_bounds(
        RASTER_1,
        gdf
    )
    yield gdf_clip

@pytest.fixture
def grid(gdf_clip):
    out_grid = building_zonals.rasterise_clip(RASTER_1, gdf_clip)
    yield out_grid
    


def test_convert_shp_to_gpkg():
    if not OUT_GPKG.exists():
        gdf = building_zonals.convert_shp_to_gpkg(SHP, OUT_GPKG, OUT_LAYER)
        assert OUT_GPKG.exists()
        gdf_shp = gpd.read_file(SHP)
        gdf_gpkg = gpd.read_file(OUT_GPKG, layer=OUT_LAYER)
        assert len(gdf_shp) == len(gdf_gpkg)
        assert 'tile_name' in gdf_gpkg.columns


def test_get_buildings_using_bounds(gdf_clip):
    gdf = gpd.read_file(OUT_GPKG, layer=OUT_LAYER)
    src = rasterio.open(RASTER_1)
    raster_bounds = list(src.bounds)
    gdf_clip_bounds = list(gdf_clip.total_bounds)
    assert raster_bounds == gdf_clip_bounds


def test_rasterise_clip(grid):
    assert list(grid.data_vars.keys()) == ['osm_id', 'heights']


def test_get_building_height_stats(grid):
    # might need some additional functions
    df = building_zonals.get_building_height_stats(grid, ["mean"])
    if not ZONALS_CSV.exists():
        df.to_csv(ZONALS_CSV, index=False)
    assert isinstance(df, pd.DataFrame)


def test_join_csvs_and_aggregate():
    pass

def test_find_missing_buildings():
    gdf = gpd.read_file(OUT_GPKG, layer=OUT_LAYER)
    gdf['tile_name'] = 'TQ38'
    gdf_missing = building_zonals.find_missing_buildings(gdf, ZONALS_CSV)
    df = pd.read_csv(ZONALS_CSV)
    df_missing = df[df.osm_id.isin(gdf_missing.osm_id)]
    assert df_missing.empty
    assert len(gdf_missing) == 22910


def test_sample_missing_buildings_and_join_back_to_csv():
    gdf = gpd.read_file(OUT_GPKG, layer=OUT_LAYER)
    df = building_zonals.sample_missing_buildings_and_join_back_to_csv(
        gdf,
        ZONALS_CSV,
        DATA_DIR.joinpath('rasters')
    )
    df_na = df[df.heights_mean.isna()]
    df.to_csv(DATA_DIR.joinpath('CHECKED_THIS.csv'), index=False)
    assert len(df_na) < 22910


def test_join_csv_to_global_shape():
    pass





