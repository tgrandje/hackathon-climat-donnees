# -*- coding: utf-8 -*-
"""
Extraction des données sur les installations classées depuis Géorisques
"""

import io
import logging
import os
from zipfile import ZipFile

import geopandas as gpd
import pandas as pd
from requests_cache import CachedSession
from tqdm import tqdm

from hackathon_climat_donnees.constants import GEREP_THRESHOLDS

logger = logging.getLogger(__name__)

CACHE_DURATION_SECONDS = 60 * 60 * 24 * 10
SESSION = CachedSession(
    "cache",
    backend="sqlite",
    expire_after=CACHE_DURATION_SECONDS,
    allowable_methods=("GET", "POST"),
)

GEORISQUES_SERVICES = "https://www.georisques.gouv.fr/themes/custom/georisques/assets/dist/js/georisques_commun/web-service-urls.json"
webservices = SESSION.get(GEORISQUES_SERVICES).json()
download_url = webservices["DOWNLOAD"]


def to_disk(gdf: gpd.GeoDataFrame) -> None:
    # export multi-format
    os.makedirs("./output")
    gdf.to_file("./output/sample.gpkg", driver="GPKG")
    gdf.to_file("./output/sample.shp")
    gdf = gdf.copy()
    gdf.drop("geometry", axis=1).to_csv("./output/sample.csv", sep=";")
    gdf.to_file("./output/sample.geojson", driver="GeoJSON")


def prepare_dataset() -> gpd.GeoDataFrame:
    """
    Identification des principales ICPE métropolitaines. Utilise la base
    de données "Installations industrielles" de Géorisques

    La sélection opérée est :
        * SEVESO seuil haut
        * classé en priorité nationale
        * en métropole
        * soumis à la directive IED

    Returns
    -------
    gdf : gpd.GeoDataFrame
        GeoDataFrame des principales ICPE métropolitaines

    """

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


def profile_irep() -> gpd.GeoDataFrame:
    """
    Evalue des "profils" des ICPE à partir des données IREP de Géorisques :
    préleveur majeur, volume rejeté important, fortes émissions atmosphériques,
    ...

    Permet d'évaluer en première approche les sujets prioritaires pour les ICPE
    les plus impactantes de France.

    Les calculs sont faits en prenant en compte les émissions médianes
    déclarées sur les 5 dernières années pour chaque établissement, puis
    (généralement) en sélectionnant le percentile 75 de la métrique considérée.

    * prelevements en eau (lien avec la réduction des volumes disponibles):
      en première approximation, on définit un préleveur majeur comme une
      ICPE qui se situe dans le 1er quartile annuel, hors prélèvements en
      mer

    * rejets en eau (lien potentiel avec le rejet de chaleur dans des cours
      d'eau déjà réchauffés)
      en première approximation, on définit un rejet majeur comme une ICPE
      qui se situe dans le 1er quartile annuel, hors rejets en réseau

    * rejets atmosphériques (lien avec l'altération du vent)
      calcul d'un score environnemental approximatif (serait à améliorer pour
      ne prendre en compte que les substances à réel impact sanitaire) puis
      sélection du top 25%

    * rejets aqueux (lien avec la réduction des débits d'étiage, entraînant
      une augmentation de l'impact des rejets avec un facteur de dilution
      moindre)
      calcul d'un score environnemental parmi les rejets directs au milieu (par
      opposition aux rejets en réseau d'assainissement) puis sélection du top 25%

    * rejets sur les sols = épandages (lien avec le changement des
      précipitations pouvant impacter percolation et ruissellement)
      calcul d'un score environnemental puis sélection du top 25%

    Returns
    -------
    gpd.GeoDataFrame
        Données IREP des ICPE
        Ex:
            identifiant                               nom_etablissement    numero_siret  \
          0  0000000039  COLLECTES VALORISATION ENERGIE DECHETS - COVED  34340353103542   
          1  0003012003                                        NOVAWOOD  82181865500010   

                                                       adresse code_insee  \
          0  AVENUE DES EOLIENNES   LE RAZAS 26780 MALATAVERNE      26169   
          1  chemin du Vaquené 54410 LANEUVEVILLE-DEVANT-NANCY      54300   

            code_departement code_region                        geometry PLV_Q75  \
          0              26           84  POINT (839475.211 6374347.037)   False   
          1              54           44  POINT (940538.885 6842661.186)   False   

            VOLREJ_Q75 AIR_Q75 EAU_Q75 SOL_Q75  
          0      False   False   False   False  
          1      False    True   False   False  

    """

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
    df = gpd.GeoDataFrame(df, crs=2154)
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
    df = df.to_frame("PLV_Q75").reset_index(drop=False)
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
    df = df.to_frame("VOLREJ_Q75").reset_index(drop=False)
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
    df = df.to_frame("AIR_Q75").reset_index(drop=False)
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
    df = df.to_frame("EAU_Q75").reset_index(drop=False)
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
    df = df.to_frame("SOL_Q75").reset_index(drop=False)
    dict_df["emissions_sol"] = df

    del dict_df["emissions"]

    etabs = dict_df.pop("etablissements")
    for key, meta in dict_df.items():
        etabs = etabs.merge(meta, on="identifiant", how="left")

    for col in ["PLV_Q75", "VOLREJ_Q75", "AIR_Q75", "EAU_Q75", "SOL_Q75"]:
        ix = etabs[etabs[col].isnull()].index
        etabs.loc[ix, col] = False

    return etabs


def merge_datasets(gdf, irep_profiles) -> gpd.GeoDataFrame:
    # TODO : docstring
    gdf = gdf.rename({"geometry": "geometry_icpe"}, axis=1)
    gdf = gdf.merge(
        irep_profiles, left_on="code_aiot", right_on="identifiant", how="left"
    ).drop("identifiant", axis=1)

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

    # prendre l'adresse et le code commune, dep, region, noms déclarés dans IREP
    gdf = gdf.drop(["adresse_x", "num_dep", "cd_insee", "nom_ets"], axis=1)
    gdf = gdf.rename({"adresse_y": "adresse"}, axis=1)

    # Laisser de côté les ICPE pour lesquelles on n'a pas de géolocalisation
    gdf = gdf[~gdf.geometry.isnull()]
    gdf = gdf.set_crs(2154)

    # Réévaluer x & y à partir des géométries
    gdf["x"] = gdf.geometry.x
    gdf["y"] = gdf.geometry.x
    gdf["code_epsg"] = 2154

    # Renommer pour éviter les problèmes de troncature au format shp
    gdf = gdf.rename(
        {
            "nom_etablissement": "nom",
            "numero_siret": "siret",
            "code_departement": "dept",
            "code_region": "region",
        },
        axis=1,
    )

    return gdf


def hazards(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Récupère les risques identifiés aux coordonnées des ICPE.
    Utilise l'API "Rapport PDF et JSON" de géorisques, résultats similaires
    au rapport "risques près de chez moi".

    Parameters
    ----------
    gdf : gpd.GeoDataFrame
        GeoDataFrame des ICPE.

    Returns
    -------
    gdf : gpd.GeoDataFrame
        GeoDataFrame des risques retournés par l'API

        Encodage des niveaux de risques selon le dictionnaire suivant :
        {
            "Risque Inconnu": 1,
            "Risque Existant": 2,
            "Risque Existant - faible": 3,
            "Risque Existant - modéré": 4,
            "Risque Existant - important": 5,
        }
        
        Ex.:
            libelle     Avalanche  Feu de foret  Inondation  Recul du trait de cote  \
            code_aiot                                                                 
            0003205293          0             0           0                       0   
            0003300469          0             0           0                       0   

            libelle     Remontée de nappe  Retrait gonflement des argiles  \
            code_aiot                                                       
            0003205293                  1                               3   
            0003300469                  2                               4   

            libelle     Risques côtiers (submersion marine, tsunami)  
            code_aiot                                                 
            0003205293                                             0  
            0003300469                                             0  

    """
    endpoint = "https://georisques.gouv.fr/api/v1/resultats_rapport_risque"
    gdf = gdf.to_crs(4326)

    data = []
    for site in tqdm(gdf.itertuples(False), total=len(gdf)):
        x = site.geometry.x
        y = site.geometry.y
        lonlat = f"{x},{y}"
        r = SESSION.get(endpoint, params={"latlon": lonlat})
        natural_hazard = [
            detail
            for haz, detail in r.json()["risquesNaturels"].items()
            if detail.pop("present")
        ]
        [d.update({"code_aiot": site.code_aiot}) for d in natural_hazard]
        data += natural_hazard
    data = pd.DataFrame(data)

    encoding = {
        "Risque Inconnu": 1,
        "Risque Existant": 2,
        "Risque Existant - faible": 3,
        "Risque Existant - modéré": 4,
        "Risque Existant - important": 5,
    }
    data["libelleStatutAdresse"] = data["libelleStatutAdresse"].map(encoding)
    data = (
        data.pivot_table(
            "libelleStatutAdresse",
            columns="libelle",
            index="code_aiot",
            aggfunc="sum",
        )
        .fillna(0)
        .astype(int)
    )
    for col in data.columns:
        if (data[col] == 0).all():
            data = data.drop(col, axis=1)

    # filtrer les risques climatiques liés aux conditions météo
    drop = ["Mouvements de terrain", "Radon", "Séisme"]
    data = data.drop(drop, axis=1)
    return data


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    gdf = prepare_dataset()
    irep_profiles = profile_irep()
    gdf = merge_datasets(gdf, irep_profiles)

    # Faire le calcul des risques sur les géométries déclarées dans IREP
    hazards = hazards(gdf)
    gdf = gdf.merge(hazards, on="code_aiot", how="left")

    # renommage pour éviter les troncatures SHP
    gdf = gdf.rename(
        {
            "Feu de foret": "feu foret",
            "Recul du trait de cote": "recul tdc",
            "Remontée de nappe": "in. nappe",
            "Retrait gonflement des argiles": "argiles",
            "Risques côtiers (submersion marine, tsunami)": "risq. cot.",
        },
        axis=1,
    )

    to_disk(gdf)
