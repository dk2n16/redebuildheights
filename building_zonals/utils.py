"""Utility functions"""

from pathlib import Path 
from typing import Union, List

from geocube.api.core import make_geocube
import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
import rioxarray
from shapely.geometry import box
import xarray

GRID_GPKG = Path(__file__).resolve().parent.joinpath('OS_BNG_10km.gpkg')

def convert_shp_to_gpkg(
        shp: Union[Path, str], 
        gpkg: Union[Path, str],
        layer: str) -> None:
    """Converts shp shapefile to geopackage and gets grid id in which building lies
    
    Args:
    shp: Shapefile path
    gpkg: Geopackage path
    layer: Layer in geopackage

    Returns:
    None
    """
    gdf = gpd.read_file(shp).to_crs(27700)
    gdf_grid = gpd.read_file(GRID_GPKG)
    gdf['centroids'] = gdf.centroid
    gdf = gdf.set_geometry('centroids')
    gdf = gdf.sjoin(gdf_grid, how='left', predicate='intersects')
    gdf = gdf.set_geometry('geometry')
    gdf = gdf[['osm_id', 'name', 'type', 'tile_name', 'geometry']]
    gdf.osm_id = gdf.osm_id.astype(np.int32)
    gdf.to_file(gpkg, layer=layer, index=False)
    return gdf

def get_buildings_using_bounds(
        raster: Union[Path, str],
        gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Gets bounds of raster and returns gdf of gpkg clipped to bounds

    Args:
    raster: Raster path
    gdf: Buildings geodataFrame

    Returns:
    gdf: Geodataframe of buildings clipped to raster
    """
    src = rasterio.open(raster)
    bounds = list(src.bounds)
    polygon = box(*bounds)
    gdf_clip = gdf.clip(polygon)
    return gdf_clip

def rasterise_clip(
    raster: Union[Path, str],
    gdf: gpd.GeoDataFrame,
) -> xarray.DataArray:
    """
    Rasterises gdf like raster and returns raster

    Args:
    raster: Path to raster
    gdf: gdf of buildings to rasterise

    Returns:
    out_grid: rasterised gdf dataset merged with raster data
    """
    rx = rioxarray.open_rasterio(raster)
    out_grid = make_geocube(
            vector_data=gdf, 
            measurements=['osm_id'], 
            like=rx)
    out_grid['heights'] = (rx.dims, rx.values, rx.attrs, rx.encoding)
    return out_grid

def get_building_height_stats(
    raster_dataset: xarray.core.dataset.Dataset,
    stats: List[str]
) -> pd.DataFrame:
    """Calculates zonal statistics for height band inside each osm_id band of raster_dataset
    
    Args:
    raster_dataset : 2 band dataset containing osm_id and heights bands
    stats : stats to calculate (options ['mean', 'min', 'max', 'med']) - Must have a list of at least on of these

    Returns:
    df : DataFrame of statistics
    """
    stats_for_dataframe = []
    grouped_heights = raster_dataset.drop_vars("spatial_ref").groupby(raster_dataset.osm_id)
    if "mean" in stats:
        #grid_mean = grouped_heights.mean().rename({"heights": "heights_mean"})
        stats_for_dataframe.append(grouped_heights.mean().rename({"heights": "heights_mean"}))
    if "min" in stats:
        stats_for_dataframe.append(grouped_heights.min().rename({"heights": "heights_min"}))
    if "max" in stats:
        stats_for_dataframe.append(grouped_heights.max().rename({"heights": "heights_max"}))
    if "med" in stats:
        stats_for_dataframe.append(grouped_heights.median().rename({"heights": "heights_med"}))

    df = xarray.merge(stats_for_dataframe).to_dataframe().reset_index()
    df = df[[x for x in df.columns if not x in ['band', 'spatial_ref']]]
    stats_cols_to_insert = ["heights_mean", "heights_min", "heights_max", "heights_med"]
    for i in stats_cols_to_insert: # holder columns for aggregation later
        if not i in df.columns:
            df[i] = np.nan
    return df


def find_missing_buildings(
        gdf: gpd.GeoDataFrame,
        csv: Union[Path, str]) -> gpd.GeoDataFrame:
    """Returns gdf of buildings in gpkg missing in csv
    
    Args:
    gdf: gpd.GeoDataFrame
    csv : Path to csv

    Returns:
    gdf : Dataframe of missing buildings
    """
    gdf = gdf.set_index('osm_id')
    df = pd.read_csv(csv).set_index('osm_id')
    gdf['centroid'] = gdf.centroid
    gdf = gdf.set_geometry('centroid')
    df = df[[x for x in df.columns if not x == 'tile_name']]
    gdf = gdf.join(df, how='left').reset_index()
    heights_col = [x for x in df.columns if x.startswith('heights')][0]
    gdf = gdf[gdf[heights_col].isna()]
    return gdf


def sample_missing_buildings_and_join_back_to_csv(
    gdf: gpd.GeoDataFrame,
    csv: Union[Path, str],
    raster_dir: Union[Path, str]
) -> pd.DataFrame:
    """Samples missing buildings to rasters to fill in vals
    
    Args:
    gdf: Buildings geodataframe
    csv: Path to zonals csv
    raster_dir: Folder in which rasters are held

    Returns:
    -------
    df: DataFrame with missing values filled
    """
    gdf_missing = find_missing_buildings(gdf, csv)
    gdf_missing = gdf_missing[[x for x in gdf_missing.columns if not x in ['fid', 'code', 'fclass', 'name', 'type', 'geometry',
       ]]]
    grid_ids = list(gdf_missing.tile_name.unique())
    for id in grid_ids:
        raster = raster_dir.joinpath(f'{id}.tif')
        raster = raster_dir.joinpath(f'DSM_DTM_{id}_m100_10K_Tile.tif')
        if raster.exists():
            gdf_subset = gdf_missing[gdf_missing.tile_name == id]
            gdf_missing = gdf_missing[gdf_missing.tile_name != id]
            coords = [(x,y) for x, y in zip(gdf_subset.centroid.x, gdf_subset.centroid.y)]
            src = rasterio.open(raster)
            nodata = src.nodata
            pixel_samples = [x[0] for x in src.sample(coords)]
            height_cols = [x for x in gdf_subset.columns if x.startswith('heights')]
            for col in height_cols:
                gdf_subset[col] = pixel_samples
                gdf_subset.loc[gdf_subset[col]==nodata, col] = np.nan
            gdf_missing = pd.concat([gdf_missing, gdf_subset])
    df = gdf_missing[[x for x in gdf_missing.columns if not x in ['tile_name','centroid']]]
    return df







    





