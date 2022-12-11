"""Chunk buildings into tiles and move rasters to same dir and process"""

from pathlib import Path
import building_zonals as bz

BASE = Path(__file__).resolve().parent
GPKG = Path(r'C:\Users\dkerr\Documents\GISRede\buildings\UK\London\data\building_heights_tiles\buildings.gpkg').resolve()
DATA_DIR = GPKG.parent.joinpath('rasters')
building_shp = DATA_DIR.joinpath('gis_osm_buildings_a_free_1.shp')


def main():
    #chunk_buildings_and_move_rasters()
    for index, tile in enumerate(DATA_DIR.joinpath('tiles').iterdir()):
        print(tile.name)
        building_gpkg = tile.joinpath(f'{tile.name}.gpkg')
        building_layer = 'buildings_uk'
        building_id_field = 'osm_id'
        building_crs = 27700
        raster_dir = tile.joinpath(tile.name)
        stats = ['mean', 'med']
        output_gpkg = building_gpkg
        output_layer = 'building_heights'
        bz.BuildingHeightsSingle(
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
        print()
        if index == 1:
            break
        


def chunk_buildings_and_move_rasters():
    out_parent = DATA_DIR.joinpath('tiles')
    raster_dir = DATA_DIR
    for cell, tile_name in bz.iterate_grid_cells():
        gdf_clip = bz.extract_from_buildings(GPKG, 'buildings_uk', cell)
        if not out_parent.joinpath(tile_name, f'{tile_name}.gpkg').exists():
            bz.save_gpkg_to_folder(gdf_clip, out_parent, raster_dir, tile_name)


if __name__ == "__main__":
    main()

