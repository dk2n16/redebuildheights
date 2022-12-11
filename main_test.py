from pathlib import Path
import pytest
import fiona

from concurrent.futures import ProcessPoolExecutor
import building_zonals as bz

DATA_DIR = Path(__file__).resolve().parent.joinpath('tests/data')

SHP = DATA_DIR.joinpath('buildings_uk.shp')
RASTER_1 = DATA_DIR.joinpath('rasters/TQ38.tif')
RASTER_2 = DATA_DIR.joinpath('rasters/TQ38_2.tif')

OUT_GPKG = DATA_DIR.joinpath('buildings_uk.gpkg')
OUT_LAYER = 'buildings_uk'
ZONALS_CSV = DATA_DIR.joinpath('TQ38.csv')
HEIGHTS_LAYER = 'building_heights'

def main_():
    x = bz.BuildingHeightsMulti(
        SHP,
        OUT_GPKG,
        OUT_LAYER,
        'osm_id', 
        27700, 
        DATA_DIR.joinpath('rasters'),
        ['mean'],
        OUT_GPKG,
        HEIGHTS_LAYER,
        n_workers=3)

def main():
    #data_to_process = list(get_data())
    #data_from_gen = get_data()
    with ProcessPoolExecutor(max_workers=5) as exec:
        for x in get_data():
            result = exec.submit(process, x)
        #result = exec.map(process, data_to_process)


def process(mes_tup):
    import time
    time.sleep(1)
    print(mes_tup[0])
    print(mes_tup[1])

def get_data():
    import time
    for i in range(10):
        time.sleep(1)
        name = f'David{i}'
        message = f'Hello{i}'
        yield name, message



if __name__ == "__main__":
    from datetime import datetime
    start = datetime.now()
    main()
    finish = datetime.now()
    print(f'it took {finish - start}')