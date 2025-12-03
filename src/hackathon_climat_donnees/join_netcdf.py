#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Jointure des fichiers netcdf et icpe
"""
import pandas as pd
import geopandas as gpd

from hackathon_climat_donnees.prep_datasets import prep_dataset_icpe


def parse_netcdf_csv(path="tas_max_RP_GEV_points_all_period_ALL_COLUMNS.csv"):
    df = pd.read_csv(path, sep=",")
    geoms = gpd.GeoSeries(
        gpd.points_from_xy(df["x"], df["y"]), name="geometry", crs=2154
    )
    df = df.join(geoms).drop(["x", "y"], axis=1)
    return gpd.GeoDataFrame(df, crs=2154)


def join(gdf, meteo):
    gdf = gpd.sjoin_nearest(gdf, meteo, how="left")
    return gdf


if __name__ == "__main__":
    gdf = prep_dataset_icpe()
    meteo = parse_netcdf_csv()
    test = join(gdf, meteo)
