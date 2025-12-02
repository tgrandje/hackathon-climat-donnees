# -*- coding: utf-8 -*-
"""
Extraction des données sur les installations classées depuis Géorisques
"""

import geopandas as gpd
import pandas as pd

from french_cities import find_city

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

    gdf.to_file("sample.gpkg", driver="GPKG")

    return gdf


if __name__ == "__main__":
    df = prepare_dataset()
