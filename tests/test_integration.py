from pathlib import Path
import pytest
import fiona

import building_zonals as bz

DATA_DIR = Path(__file__).resolve().parent.joinpath('data')

SHP = DATA_DIR.joinpath('buildings_uk.shp')
RASTER_1 = DATA_DIR.joinpath('rasters/TQ38.tif')
RASTER_2 = DATA_DIR.joinpath('rasters/TQ38_2.tif')

OUT_GPKG = DATA_DIR.joinpath('buildings_uk.gpkg')
OUT_LAYER = 'buildings_uk'
ZONALS_CSV = DATA_DIR.joinpath('TQ38.csv')
HEIGHTS_LAYER = 'building_heights'

@pytest.fixture
def build():
    x = bz.BuildingHeightsSingle(
            SHP,
            OUT_GPKG,
            OUT_LAYER,
            'osm_id', 
            27700, 
            DATA_DIR.joinpath('rasters'),
            ['mean'],
            OUT_GPKG,
            HEIGHTS_LAYER)
    yield x

def test_instantiation(build):
    assert isinstance(build, bz.BuildingHeightsSingle)
    assert build.building_id_field == 'osm_id'
    assert build.building_crs == 27700

def test_gpkg_creation(build):
    assert build.building_gpkg.exists()

def test_integration(build):
    assert 'building_heights' in fiona.listlayers(OUT_GPKG)
