"""Module with class to carry out functionality to calculate zonals in multiple tiles"""

from dataclasses import dataclass
from pathlib import Path
from typing import Union, List, Optional, Tuple
from concurrent.futures import ProcessPoolExecutor, as_completed

import geopandas as gpd
import pandas as pd

import building_zonals as bz


@dataclass
class BuildingHeightsSingle:
    """Processes zonal stats using single core"""
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
        gdf = self.get_geoms()
        print('GOT BUILDINGS')
        for gdf_clip, raster in self.get_blocks(gdf):
            self.get_building_heights(gdf_clip, raster)
        self.make_zonals_table()
        df_missing, gdf_overlapping = bz.sample_missing_buildings_and_join_back_to_csv(
            gdf,
            self.raster_dir.joinpath('tmp/ZONALS.csv'),
            self.raster_dir
        )
        print('GOT MISSING BUILDINGS')
        zonals_df = pd.read_csv(self.raster_dir.joinpath('tmp/ZONALS.csv'))
        final_df = pd.concat([zonals_df, df_missing])
        final_df = final_df.groupby(self.building_id_field).agg({
                "heights_mean": ['mean'],
                "heights_min": ['min'],
                "heights_max": ['max'],
                "heights_med": ['median']})
        final_df.columns = ["heights_mean", "heights_min", "heights_max", "heights_med"]
        final_df.to_csv(self.output_gpkg.parent.joinpath('BUILDING_ZONALS.csv'))
        if self.save_output_gpkg and self.output_gpkg and self.output_layer:
            print('SAVING BUILDINGS')
            gdf = gdf.set_index(self.building_id_field)
            gdf = gdf[[x for x in gdf.columns if not x in ["heights_mean", "heights_min", "heights_max", "heights_med"]]]
            gdf_join = gdf.join(final_df, how='inner')
            gdf_join = gdf_join[['name', 'type', 'tile_name', "heights_mean", "heights_min", "heights_max", "heights_med", "geometry"]]
            gdf_join.to_file(self.output_gpkg, layer=self.output_layer)
            gdf_overlapping.to_file(self.output_gpkg, layer="overlapping_buildings")



    def make_zonals_table(self):
        """Append all csvs into one"""
        tmp_csv_folder = self.raster_dir.joinpath('tmp')
        ZONALS_TABLE = tmp_csv_folder.joinpath('ZONALS.csv')
        df_list = [pd.read_csv(x) for x in tmp_csv_folder.iterdir() if x.name.endswith('.csv') if not x.name=='ZONALS.csv']
        zonals_df = pd.concat(df_list)
        zonals_df.to_csv(ZONALS_TABLE, index=False)
        

    
    def get_building_heights(
        self,
        gdf_clip: gpd.GeoDataFrame,
        raster: Union[Path, str]):
        """Rasterises, calculates heights and saves csv
        
        Args:
        gdf_clip: Buildings clipped to raster
        raster: Path to raster

        Returns:
        None
        """
        tmp_csv_folder = self.raster_dir.joinpath('tmp')
        if not tmp_csv_folder.exists():
            tmp_csv_folder.mkdir()
        out_csv = tmp_csv_folder.joinpath(f'{raster.stem}.csv')
        if not out_csv.exists():
            grid = bz.rasterise_clip(raster, gdf_clip)
            df = bz.get_building_height_stats(grid, self.stats)
            df['tile_name'] = raster.name.split('_')[2]
            df.to_csv(out_csv, index=False)


    def get_blocks(self, gdf: gpd.GeoDataFrame) -> Tuple[gpd.GeoDataFrame, Union[Path, str]]:
        """Yields each geodataframe clipped to bounds of each raster
        
        Args:
        gdf: Buildings geodataframe

        Yields:
        gdf_clip : GeoDataFrame clipped to raster
        raster: Path to raster
        """
        rasters = [x for x in self.raster_dir.iterdir() if x.name.endswith('.tif')]
        for raster in rasters:
            gdf_clip = bz.get_buildings_using_bounds(
                    raster,
                    gdf
                )
            yield gdf_clip, raster        
        

    def get_geoms(self) -> gpd.GeoDataFrame:
        """Opens/converts buildings to geodataframe
        
        Args:
        self: class

        Returns:
        gdf: GeoDataFrame of buildings
        """
        if not Path(self.building_gpkg).resolve().exists():
            gdf = bz.convert_shp_to_gpkg(
                    self.building_shp,
                    self.building_gpkg, 
                    self.building_layer)
        else:
            gdf = gpd.read_file(self.building_gpkg, layer=self.building_layer)
        return gdf

            

@dataclass
class BuildingHeightsMulti:
    """Processes zonal stats using single core"""
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
    n_workers: Optional[int] = 2

    def __post_init__(self):
        gdf = self.get_geoms()
        print('GOT BUILDINGS')
        with ProcessPoolExecutor(max_workers=self.n_workers) as exec:
            for gdf_clip, raster in self.get_blocks(gdf):
                future = exec.submit(self.get_building_heights, gdf_clip, raster)
        self.make_zonals_table()
        df_missing, gdf_overlapping = bz.sample_missing_buildings_and_join_back_to_csv(
            gdf,
            self.raster_dir.joinpath('tmp/ZONALS.csv'),
            self.raster_dir
        )
        print('GOT MISSING BUILDINGS')
        zonals_df = pd.read_csv(self.raster_dir.joinpath('tmp/ZONALS.csv'))
        final_df = pd.concat([zonals_df, df_missing])
        final_df = final_df.groupby(self.building_id_field).agg({
                "heights_mean": ['mean'],
                "heights_min": ['min'],
                "heights_max": ['max'],
                "heights_med": ['median']})
        final_df.columns = ["heights_mean", "heights_min", "heights_max", "heights_med"]
        final_df.to_csv(self.output_gpkg.parent.joinpath('BUILDING_ZONALS.csv'))
        if self.save_output_gpkg and self.output_gpkg and self.output_layer:
            print('SAVING BUILDINGS')
            gdf = gdf.set_index(self.building_id_field)
            gdf = gdf[[x for x in gdf.columns if not x in ["heights_mean", "heights_min", "heights_max", "heights_med"]]]
            gdf_join = gdf.join(final_df, how='inner')
            gdf_join = gdf_join[['name', 'type', 'tile_name', "heights_mean", "heights_min", "heights_max", "heights_med", "geometry"]]
            gdf_join.to_file(self.output_gpkg, layer=self.output_layer)
            gdf_overlapping.to_file(self.output_gpkg, layer="overlapping_buildings")



    def make_zonals_table(self):
        """Append all csvs into one"""
        tmp_csv_folder = self.raster_dir.joinpath('tmp')
        ZONALS_TABLE = tmp_csv_folder.joinpath('ZONALS.csv')
        df_list = [pd.read_csv(x) for x in tmp_csv_folder.iterdir() if x.name.endswith('.csv') if not x.name=='ZONALS.csv']
        zonals_df = pd.concat(df_list)
        zonals_df.to_csv(ZONALS_TABLE, index=False)
        

    
    def get_building_heights(
        self,
        gdf_clip: gpd.GeoDataFrame,
        raster: Union[Path, str]):
        """Rasterises, calculates heights and saves csv
        
        Args:
        gdf_clip: Buildings clipped to raster
        raster: Path to raster

        Returns:
        None
        """
        tmp_csv_folder = self.raster_dir.joinpath('tmp')
        if not tmp_csv_folder.exists():
            tmp_csv_folder.mkdir()
        out_csv = tmp_csv_folder.joinpath(f'{raster.stem}.csv')
        if not out_csv.exists():
            grid = bz.rasterise_clip(raster, gdf_clip)
            df = bz.get_building_height_stats(grid, self.stats)
            df['tile_name'] = raster.name.split('_')[2]
            df.to_csv(out_csv, index=False)


    def get_blocks(self, gdf: gpd.GeoDataFrame) -> Tuple[gpd.GeoDataFrame, Union[Path, str]]:
        """Yields each geodataframe clipped to bounds of each raster
        
        Args:
        gdf: Buildings geodataframe

        Yields:
        gdf_clip : GeoDataFrame clipped to raster
        raster: Path to raster
        """
        rasters = [x for x in self.raster_dir.iterdir() if x.name.endswith('.tif')]
        for raster in rasters:
            gdf_clip = bz.get_buildings_using_bounds(
                    raster,
                    gdf
                )
            yield gdf_clip, raster        
        

    def get_geoms(self) -> gpd.GeoDataFrame:
        """Opens/converts buildings to geodataframe
        
        Args:
        self: class

        Returns:
        gdf: GeoDataFrame of buildings
        """
        if not Path(self.building_gpkg).resolve().exists():
            gdf = bz.convert_shp_to_gpkg(
                    self.building_shp,
                    self.building_gpkg, 
                    self.building_layer)
        else:
            gdf = gpd.read_file(self.building_gpkg, layer=self.building_layer)
        return gdf

