#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Jointure des fichiers netcdf et icpe
"""
import geopandas as gpd
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


# if __name__ == "__main__":
#     from hackathon_climat_donnees import INPUT
#     import os

#     gdf = prep_dataset_icpe()
#     meteo = os.path.join(INPUT, "tasmax_RP_GEV_TRACC2-7.nc")
#     test = join_dataset_meteo(gdf, meteo)
