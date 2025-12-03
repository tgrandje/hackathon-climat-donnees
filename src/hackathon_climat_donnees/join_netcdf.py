#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Jointure des fichiers netcdf et icpe
"""
import geopandas as gpd
import pandas as pd
import xarray as xr

from hackathon_climat_donnees.prep_datasets import prep_dataset_icpe


def parse_netcdf_to_dataframe(path: str) -> gpd.GeoDataFrame:
    """
    Convertit un fichier netcdf issu du module netcdfprocessing en GeoDataFrame

    Parameters
    ----------
    path : str
        Chemin vers le fichier netcdf.

    Returns
    -------
    df : gpd.GeoDataFrame
        GeoDataFrame des données netcdf.

    """
    df = (
        xr.open_dataset(path)
        .drop_dims("gev_params")
        .stack(points=("y", "x", "periods"))
        .to_dataframe()
        .reset_index()
        .dropna()
        .pivot(
            index=["x", "y", "lat", "lon"],
            columns="periods",
            values="return_levels",
        )
        .reset_index()
    )
    geoms = gpd.GeoSeries(
        gpd.points_from_xy(df["x"], df["y"]), name="geometry", crs=2154
    )
    df = df.join(geoms).drop(["x", "y"], axis=1)
    df = gpd.GeoDataFrame(df, crs=2154)
    return df


def join_dataset_meteo(
    gdf: gpd.GeoDataFrame, path_netcdf: str
) -> gpd.GeoDataFrame:
    """
    Jointure d'un dataset (ICPE) à un scenario météo

    Parameters
    ----------
    gdf : gpd.GeoDataFrame
        GeoDataFrame du semi de point étudié (ici les ICPE)
    path_netcdf : str
        Chemin vers le fichier netcdf généré par le module netcdf_processing

    Returns
    -------
    gdf : gpd.GeoDataFrame
        GeoDataFrame assemblé

        Le dataframe contient les colonnes du dataframe initial, augmenté des
        données (ici température) pour le scénario donné à
        périodes de retour 2, 5, 10, 20, 50, 100.

    """

    meteo = parse_netcdf_to_dataframe(path_netcdf)

    gdf = gpd.sjoin_nearest(gdf, meteo, how="left").drop("index_right", axis=1)
    gdf = gdf.drop(["lat", "lon"], axis=1)
    return gdf


def all_scenarii(
    gdf: gpd.GeoDataFrame, list_paths_netcdf: list[str]
) -> pd.DataFrame:
    """
    Calcul des scenarii pour chaque ICPE

    Parameters
    ----------
    gdf : gpd.GeoDataFrame
        GeoDataFrame des ICPE considérées
    list_paths_netcdf : list[str]
        Liste des chemins correspondant à chaque scenario netcdf produit par
        netcdf_processing

    Raises
    ------
    ValueError
        Dans le cas où un fichier netcdf n'est pas trouvé.

    Returns
    -------
    df : pd.DataFrame
    
    Ex.:
                                                  2          5         10  \
    code_aiot  scenario                                                     
    0003205293 tasmax_RP_GEV_TRACC2-7.nc  33.745651  35.813251  37.049598   
               tasmax_RP_GEV_TRACC2.nc    32.628636  34.973529  36.354923   
    0003300469 tasmax_RP_GEV_TRACC2-7.nc  33.745651  35.813251  37.049598   
               tasmax_RP_GEV_TRACC2.nc    32.628636  34.973529  36.354923   
    
                                                 20         50        100  
    code_aiot  scenario                                                    
    0003205293 tasmax_RP_GEV_TRACC2-7.nc  38.145171  39.442737  40.333152  
               tasmax_RP_GEV_TRACC2.nc    37.564950  38.979659  39.938044  
    0003300469 tasmax_RP_GEV_TRACC2-7.nc  38.145171  39.442737  40.333152  
               tasmax_RP_GEV_TRACC2.nc    37.564950  38.979659  39.938044  

    """
    all_gdfs = []
    for path in list_paths_netcdf:
        if not os.path.exists(path):
            raise ValueError("file not found at %s", path)
        filename = os.path.basename(path)
        gdf_with_scenario = join_dataset_meteo(gdf, path)
        gdf_with_scenario["scenario"] = filename
        all_gdfs.append(gdf_with_scenario)

    gdf = gpd.pd.concat(all_gdfs)

    cols = [2, 5, 10, 20, 50, 100]
    df = gdf.set_index(["code_aiot", "scenario"])[cols]
    df = df.rename(columns={x: str(x) for x in cols})
    df = df.sort_index()
    return df


# if __name__ == "__main__":
#     from hackathon_climat_donnees import INPUT
#     import os

#     gdf = prep_dataset_icpe()

#     test = all_scenarii(
#         gdf,
#         [
#             os.path.join(INPUT, "tasmax_RP_GEV_TRACC2.nc"),
#             os.path.join(INPUT, "tasmax_RP_GEV_TRACC2-7.nc"),
#         ],
#     )
