"""Module with class to carry out functionality to calculate zonals in multiple tiles"""

from dataclasses import dataclass
from pathlib import Path
from typing import Union, List, Optional

import geopandas as gpd
import pandas as pd

import building_zonals as bz

@dataclass
class BuildingHeights:
    """Calculate multiple stats on building heights of rasters in a folder"""
    building_shp: Union[str, Path]
    building_gpkg: Union[str, Path]
    building_layer: str
    building_id_field: str
    building_crs: int
    raster_dir : Union[str, Path]
    stats: List[str]
    output_gpkg: Optional[Union[str, Path, None]] = None
    output_layer: Optional[Union[str, None]] = None
    save_output_gpkg: Optional[bool] = True

    def __post_init__(self):
        """Process"""
        print('GETTING BUILDINGS')
        if not Path(self.building_gpkg).resolve().exists():
            gdf = bz.convert_shp_to_gpkg(
                    self.building_shp,
                    self.building_gpkg, 
                    self.building_layer)
        else:
            gdf = gpd.read_file(self.building_gpkg, layer=self.building_layer)
        print('GOT BUILDINGS')
        rasters = [x for x in self.raster_dir.iterdir() if x.name.endswith('.tif')]
        tmp_csv_folder = self.raster_dir.joinpath('tmp')
        if not tmp_csv_folder.exists():
            tmp_csv_folder.mkdir()
        print('STARTING ZONALS')
        for raster in rasters:
            out_csv = tmp_csv_folder.joinpath(f'{raster.stem}.csv')
            if not out_csv.exists():
                gdf_clip = bz.get_buildings_using_bounds(
                    raster,
                    gdf
                )
                grid = bz.rasterise_clip(raster, gdf_clip)
                df = bz.get_building_height_stats(grid, self.stats)
                df['tile_name'] = raster.name.split('_')[2]
                df.to_csv(out_csv, index=False)
        ZONALS_TABLE = tmp_csv_folder.joinpath('ZONALS.csv')
        print('COMPLETED ZONALS')
        df_list = [pd.read_csv(x) for x in tmp_csv_folder.iterdir() if x.name.endswith('.csv') if not x.name=='ZONALS.csv']
        zonals_df = pd.concat(df_list)
        zonals_df.to_csv(ZONALS_TABLE, index=False)
        print('GETTING MISSING BUILDINGS')
        df_missing = bz.sample_missing_buildings_and_join_back_to_csv(
            gdf,
            ZONALS_TABLE,
            self.raster_dir
        )
        print('GOT MISSING BUILDINGS')
        height_col = [x for x in df_missing.columns if x.startswith('heights')][0]
        zonals_df = zonals_df[height_col].dropna()
        df_final = pd.concat([zonals_df, df_missing])
        df_final = df.groupby(self.building_id_field).agg({
                "heights_mean": ['mean'],
                "heights_min": ['min'],
                "heights_max": ['max'],
                "heights_med": ['median']})
        df_final.columns = ["heights_mean", "heights_min", "heights_max", "heights_med"]
        FINAL_TABLE = self.building_gpkg.parent.joinpath('BUILDING_ZONALS.csv')
        df_final.to_csv(FINAL_TABLE)
        print('SAVED ZONALS TABLE')
        if self.save_output_gpkg and self.output_gpkg and self.output_layer:
            print('SAVING BUILDINGS')
            gdf_join = gdf.join(df_final, how='inner')
            gdf_join = gdf_join[['name', 'type', 'tile_name', "heights_mean", "heights_min", "heights_max", "heights_med", "geometry"]]
            gdf_join.to_file(self.output_gpkg, layer=self.output_layer)

            


