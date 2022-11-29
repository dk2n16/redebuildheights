from pathlib import Path 
from datetime import datetime

import building_zonals as bz

DATA_DIR = Path(r'C:\Users\dkerr\Documents\GISRede\buildings\UK\London\data\building_heights_tiles').resolve()

def main():
    building_shp = DATA_DIR.joinpath('gis_osm_buildings_a_free_1.shp')
    building_gpkg = DATA_DIR.joinpath('buildings.gpkg')
    building_layer = 'buildings_uk'
    building_id_field = 'osm_id'
    building_crs = 27700
    raster_dir = DATA_DIR.joinpath('rasters')
    stats = ['mean', 'med']
    output_gpkg = building_gpkg
    output_layer = 'building_heights'
    bz.BuildingHeights(
        building_shp,
        building_gpkg,
        building_layer,
        building_id_field,
        building_crs,
        raster_dir,
        stats,
        output_gpkg=output_gpkg,
        output_layer=output_layer,
        save_output_gpkg=True
    )


if __name__ == "__main__":
    start = datetime.now()
    main()
    finish = datetime.now()
    print(f'TOTAL SCRIPT TOOK {finish - start}')