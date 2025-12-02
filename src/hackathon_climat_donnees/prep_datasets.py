# -*- coding: utf-8 -*-
"""
Extraction des données sur les installations classées depuis Géorisques
"""

import io
import logging
from functools import lru_cache
from zipfile import ZipFile

import geopandas as gpd
import pandas as pd
from requests_cache import CachedSession

from hackathon_climat_donnees.constants import GEREP_THRESHOLDS

logger = logging.getLogger(__name__)

SESSION = CachedSession("cache", expire_after=60 * 60 * 24 * 10)

GEORISQUES_SERVICES = "https://www.georisques.gouv.fr/themes/custom/georisques/assets/dist/js/georisques_commun/web-service-urls.json"
webservices = SESSION.get(GEORISQUES_SERVICES).json()
download_url = webservices["DOWNLOAD"]


def to_disk(gdf: gpd.GeoDataFrame) -> None:
    # export multi-format
    gdf.to_file("sample.gpkg", driver="GPKG")
    gdf.to_file("sample.shp")
    gdf = gdf.copy()
    gdf.drop("geometry", axis=1).to_csv("sample.csv", sep=";")
    gdf.to_file("sample.geojson", driver="GeoJSON")


def prepare_dataset() -> gpd.GeoDataFrame:

    files = SESSION.get(download_url + "/icpe", data={"annemin": 2003}).json()
    url = files["national"]["lien"]

    content = SESSION.get(url).content
    gdf = gpd.read_file(io.BytesIO(content))

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

    return gdf


def profile_irep() -> dict:

    files = SESSION.get(download_url + "/irep", data={"annemin": 2003}).json()
    seek = ["etablissements", "emissions", "prelevements", "rejets"]

    # Sélection des 5 derniers millésimes
    files = dict(
        sorted(
            [
                (year, data["lien"])
                for year, data in files["annuel"].items()
                if data
            ],
            key=lambda x: x[0],
        )[-5:]
    )

    logger.info("file are %s", files)

    dtype = {
        "identifiant": str,
        "code_postal": str,
        "code_insee": str,
        "code_departement": str,
        "code_region": str,
    }

    dict_df = {x: [] for x in seek}
    for year, url in files.items():
        logger.info("dl %s", url)
        r = SESSION.get(url)
        file = io.BytesIO(r.content)
        with ZipFile(file) as handle:
            read = {
                y: x for x in handle.namelist() for y in seek if y in x.lower()
            }
            for key, file in read.items():
                with handle.open(file) as dset:
                    df = io.BytesIO(dset.read())
                df = pd.read_csv(
                    df, sep=";", encoding="utf8", dtype=dtype
                ).assign(year=year)
                dict_df[key].append(df)

    for key, list_df in dict_df.items():
        df = pd.concat(list_df)
        dict_df[key] = df

    # prétraitement des données établissements
    df = dict_df["etablissements"]
    df = df.sort_values(["identifiant", "year"], ascending=[1, 0])
    df = df.groupby("identifiant").first().reset_index(drop=False)
    df["adresse"] = (
        df[["adresse", "code_postal", "commune"]]
        .fillna("")
        .agg(" ".join, axis=1)
    )
    df = df.drop(["code_postal", "commune"], axis=1)

    df["geometry"] = None
    for epsg in df.code_epsg.dropna().unique():
        ix = df[df.code_epsg == epsg].index
        geoms = gpd.GeoSeries(
            gpd.points_from_xy(
                df.loc[ix, "coordonnees_x"],
                df.loc[ix, "coordonnees_y"],
                crs=epsg,
            ),
            index=ix,
        ).to_crs(2154)
        df.loc[ix, "geometry"] = geoms
    df = gpd.GeoDataFrame(df)
    df = df.drop(["coordonnees_x", "coordonnees_y"], axis=1)
    df = df[
        [
            "identifiant",
            "nom_etablissement",
            "numero_siret",
            "adresse",
            "code_insee",
            "code_departement",
            "code_region",
            "geometry",
        ]
    ]
    dict_df["etablissements"] = df

    # prétraitement des données prelevements
    # en première approximation, on définit un préleveur majeur comme une ICPE
    # qui se situe dans le 1er quartile annuel, hors prélèvements en mer
    # lien avec la réduction des volumes disponibles

    df = dict_df["prelevements"]
    df = df.drop("prelevements_mer", axis=1)
    cols = [
        "prelevements_eaux_surface",
        "prelevements_reseau_distribution",
        "prelevements_eaux_souterraines",
    ]
    for col in cols:
        df[col] = pd.to_numeric(df[col].replace("< seuil", 0))
    df["prelevement"] = df[cols].sum(axis=1)

    df = df.groupby("identifiant")["prelevement"].median()
    q75 = df.quantile(0.75)
    df = df > q75
    df = df.to_frame("top25% prelevements").reset_index(drop=False)
    dict_df["prelevements"] = df

    # prétraitement des données rejets
    # en première approximation, on définit un rejet majeur comme une ICPE
    # qui se situe dans le 1er quartile annuel, hors rejets en réseau
    # lien potentiel avec le rejet de chaleur dans des cours d'eau déjà
    # réchauffés

    df = dict_df["rejets"]
    df = df.groupby("identifiant")["rejet_isole_m3_par_an"].median()
    q75 = df.quantile(0.75)
    df = df > q75
    df = df.to_frame("top25% rejets").reset_index(drop=False)
    dict_df["rejets"] = df

    # prétraitement des données émissions air : calcul d'un score
    # environnemental (à raffiner : devrait en théorie utiliser une score
    # de l'impact sur la santé des substances) puis sélection du top 25%
    # lien avec l'altération du vent

    df = dict_df["emissions"].query("milieu=='Air' & quantite != '< seuil'")

    thresholds = pd.Series(GEREP_THRESHOLDS["air"], name="threshold")
    df = df.merge(thresholds, left_on="polluant", right_index=True, how="left")
    df["quantite"] = pd.to_numeric(df["quantite"].replace("< seuil", 0))
    df["ratio"] = df["quantite"] / df["threshold"]

    df = (
        df.groupby(["identifiant", "polluant"])
        .ratio.median()
        .groupby("identifiant")
        .sum()
    )
    q75 = df.quantile(0.75)
    df = df > q75
    df = df.to_frame("top25% rejets atmo").reset_index(drop=False)
    dict_df["emissions_air"] = df

    # eau, rejet direct au milieu uniquement
    # calcul d'un score environnemental puis sélection du top 25%
    # lien avec la réduction des débits des cours d'eau entraînant une moinde
    # dilution

    df = dict_df["emissions"].query(
        "milieu=='Eau (direct)' & quantite != '< seuil'"
    )

    thresholds = pd.Series(GEREP_THRESHOLDS["eau"], name="threshold")
    df = df.merge(thresholds, left_on="polluant", right_index=True, how="left")
    df["quantite"] = pd.to_numeric(
        df["quantite"].replace("< seuil", 0).replace("<[valeur seuil]>", 0)
    )
    df["ratio"] = df["quantite"] / df["threshold"]

    df = (
        df.groupby(["identifiant", "polluant"])
        .ratio.median()
        .groupby("identifiant")
        .sum()
    )
    q75 = df.quantile(0.75)
    df = df > q75
    df = df.to_frame("top25% rejets aqueux").reset_index(drop=False)
    dict_df["emissions_eau"] = df

    # sols
    # calcul d'un score environnemental puis sélection du top 25%
    # lien avec le changement des précipitations (ruissellement, etc.)

    df = dict_df["emissions"].query("milieu=='Sol' & quantite != '< seuil'")
    thresholds = pd.Series(GEREP_THRESHOLDS["sol"], name="threshold")
    df = df.merge(thresholds, left_on="polluant", right_index=True, how="left")
    df["quantite"] = pd.to_numeric(
        df["quantite"].replace("< seuil", 0).replace("<[valeur seuil]>", 0)
    )
    df["ratio"] = df["quantite"] / df["threshold"]

    df = (
        df.groupby(["identifiant", "polluant"])
        .ratio.median()
        .groupby("identifiant")
        .sum()
    )
    q75 = df.quantile(0.75)
    df = df > q75
    df = df.to_frame("top25% rejets sol").reset_index(drop=False)
    dict_df["emissions_sol"] = df

    del dict_df["emissions"]

    return dict_df


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    gdf = prepare_dataset()
    profiles = profile_irep()
    gdf = gdf.rename({"geometry": "geometry_icpe"}, axis=1)
    for key, meta in profiles.items():
        gdf = gdf.merge(
            meta, left_on="code_aiot", right_on="identifiant", how="left"
        ).drop("identifiant", axis=1)
    cols = [
        "top25% prelevements",
        "top25% rejets",
        "top25% rejets atmo",
        "top25% rejets aqueux",
        "top25% rejets sol",
    ]

    # Prendre la meilleure des 2 géométries au regard de la distance
    # entre les deux : si gros écart, prendre la valeur tirée du jeu ICPE
    ix = gdf[
        gdf["geometry"]
        .set_crs(2154)
        .distance(gdf["geometry_icpe"].to_crs(2154))
        < 5_000
    ].index

    gdf.loc[ix, "geometry"] = (
        gdf.loc[ix, "geometry"]
        .set_crs(2154)
        .combine_first(gdf.loc[ix, "geometry_icpe"].to_crs(2154))
    )
    gdf = gdf.drop("geometry_icpe", axis=1)
    for col in cols:
        gdf[col] = gdf[col].fillna(False)
    to_disk(gdf)
