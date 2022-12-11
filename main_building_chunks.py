"""Chunk buildings into tiles and move rasters to same dir and process"""

from pathlib import Path
import building_zonals as bz
import pandas as pd
import geopandas as gpd
import logging
from datetime import datetime

BASE = Path(__file__).resolve().parent
GPKG = Path(r'C:\Users\dkerr\Documents\GISRede\buildings\UK\London\data\building_heights_tiles\buildings.gpkg').resolve()
DATA_DIR = GPKG.parent.joinpath('rasters')
building_shp = DATA_DIR.joinpath('gis_osm_buildings_a_free_1.shp')

logging.basicConfig(filename=BASE.joinpath('logs.log'), level=logging.INFO)

def main():
    logging.info(f'START CHUNKING - {datetime.now()}')
    chunk_buildings_and_move_rasters()
    logging.info(f'FINISED CHUNKING  - {datetime.now()}')
    for index, tile in enumerate(DATA_DIR.joinpath('tiles').iterdir()):
        building_gpkg = tile.joinpath(f'{tile.name}.gpkg')
        building_layer = 'buildings_uk'
        building_id_field = 'osm_id'
        building_crs = 27700
        raster = tile.joinpath(f'DSM_DTM_{tile.name}_m100_10K_Tile.tif')
        stats = ['mean', 'med']
        output_gpkg = building_gpkg
        output_layer = 'building_heights'
        x = bz.BuildingHeights(
            building_shp,
            building_gpkg,
            building_layer,
            building_id_field,
            building_crs,
            raster,
            stats,
            output_gpkg=output_gpkg,
            output_layer=output_layer,
            save_output_gpkg=True
        )
        logging.info(f'PROCESSING {tile.name} - {datetime.now()}')
        x.process()
        logging.info(f'FINISHED PROCESSING {tile.name} - {datetime.now()}')
    logging.info(f'MAKING ZONALS - {datetime.now()}')
    make_zonals_table()
    logging.info(f'JOINING EVERYTHING TO GPKG - {datetime.now()}')
    join_buildings_to_gpkg()

def make_zonals_table():
    df_list = [pd.read_csv(x.joinpath(f'{x.name}.csv')) for x in DATA_DIR.joinpath('tiles').iterdir() if x.name in ['TL23', 'TL24']]
    final_df = pd.concat(df_list)
    assert len(final_df) > len(final_df.osm_id.unique())
    final_df = final_df.groupby('osm_id').agg({
                "heights_mean": ['mean'],
                "heights_min": ['min'],
                "heights_max": ['max'],
                "heights_med": ['median']})
    assert len(final_df) == len(final_df.reset_index().osm_id.unique())
    final_df.columns = ["heights_mean", "heights_min", "heights_max", "heights_med"]
    final_df.to_csv(DATA_DIR.joinpath('tiles/BUILDING_ZONALS.csv'))

def join_buildings_to_gpkg():
    gdf = gpd.read_file(GPKG, layer='buildings_uk').set_index('osm_id')
    logging.info(f'JOINING EVERYTHING TO GPKG (GOT GDF) - {datetime.now()}')
    df = pd.read_csv(DATA_DIR.joinpath('tiles/BUILDING_ZONALS.csv')).set_index('osm_id')
    logging.info(f'JOINING EVERYTHING TO GPKG (GOT DF) - {datetime.now()}')
    gdf = gdf[[x for x in gdf.columns if not x in ["heights_mean", "heights_min", "heights_max", "heights_med"]]]
    gdf_join = gdf.join(df, how='inner')
    gdf_join = gdf_join[['name', 'type', 'tile_name', "heights_mean", "heights_min", "heights_max", "heights_med", "geometry"]]
    logging.info(f'SAVING HEIGHTS - {datetime.now()}')
    gdf_join.to_file(GPKG, layer='building_heights')
    logging.info(f'SAVED HEIGHTS - {datetime.now()}')
    gdf_list = []
    for tile in DATA_DIR.joinpath('tiles').iterdir():
        gdf_overlap = gpd.read_file(tile.joinpath(f'{tile.name}.gpkg'), layer='overlapping_buildings')
    gdf_overlapping = gpd.GeoDataFrame(pd.concat(gdf_list))
    logging.info(f'SAVING OVERLAPS - {datetime.now()}')
    gdf_overlapping.to_file(GPKG, layer="overlapping_buildings")        


def chunk_buildings_and_move_rasters():
    out_parent = DATA_DIR.joinpath('tiles')
    raster_dir = DATA_DIR
    for cell, tile_name in bz.iterate_grid_cells():
        logging.info(f'CHUNKING {tile_name} - {datetime.now()}')
        gdf_clip = bz.extract_from_buildings(GPKG, 'buildings_uk', cell)
        if not out_parent.joinpath(tile_name, f'{tile_name}.gpkg').exists():
            bz.save_gpkg_to_folder(gdf_clip, out_parent, raster_dir, tile_name)


if __name__ == "__main__":
    main()

