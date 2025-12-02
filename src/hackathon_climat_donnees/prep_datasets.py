# -*- coding: utf-8 -*-
"""
Extraction des données sur les installations classées depuis Géorisques
"""

import geopandas as gpd

URL = "https://mapsref.brgm.fr/wxs/georisques/georisques_dl?&service=wfs&version=2.0.0&request=getfeature&typename=InstallationsClassees&outputformat=SHAPEZIP"


def prepare_dataset():

    gdf = gpd.read_file(URL)

    # échantillon de sites SEVESO & priorité nationale
    gdf = gdf[(gdf.lib_seveso == "Seveso seuil haut") & (gdf.priorite_n == 1)]
    gdf = gdf.drop(
        ["bovins", "porcs", "volailles", "carriere", "eolienne", "industrie"],
        axis=1,
    )

    # metropole :
    gdf = gdf[gdf.cd_insee.str[:2] != "97"]

    # IED
    gdf = gdf[gdf.ied == 1]
    gdf = gdf.drop("ied", axis=1)

    rubriques = ["rubriques_", "rubrique_1", "rubrique_2"]
    gdf["rubrique"] = gdf[rubriques].fillna("").agg("|".join, axis=1)
    gdf = gdf.drop(rubriques, axis=1)

    # export multi-format
    gdf.to_file("sample.gpkg", driver="GPKG")
    gdf.to_file("sample.shp", driver="GPKG")
    gdf.drop("geometry", axis=1).to_csv("sample.csv", sep=";")

    return gdf


if __name__ == "__main__":
    df = prepare_dataset()
