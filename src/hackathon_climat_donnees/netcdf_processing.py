"""
Created on Wed Dec  3 11:31:14 2025

@author: SamyKraiem
"""

import os
import xarray as xr
import pandas as pd
import numpy as np
from scipy.stats import genextreme as gev
import time


from hackathon_climat_donnees import INPUT, OUTPUT


def RP_calcul_vectorized(extremes, periods):
    ext = extremes[~np.isnan(extremes)]
    if ext.size < 5:
        return (np.full(len(periods), np.nan), np.full(3, np.nan))

    fit = gev.fit(ext)
    return_values = gev.ppf(1 - 1 / periods, *fit)

    return return_values, np.array(fit)


# -----------------------------------------------
# 0. Paramètres
# -----------------------------------------------

PATH_DATA = "/home/azureuser/"

periods = np.array([2, 5, 10, 20, 50, 100])
VAR = "tasmaxAdjust"

# -----------------------------------------------
# 1. Charger les listes de fichiers NetCDF
# -----------------------------------------------
with open(os.path.join(INPUT, "liste_ssp370_tasmax.txt")) as f:
    ssp_files = [l.strip() for l in f.readlines()]

with open(os.path.join(INPUT, "liste_hist_tasmax.txt")) as f:
    hist_files = [l.strip() for l in f.readlines()]


def extract_gcm_rcm(path):
    parts = path.split("/")
    gcm = parts[5]
    rcm = parts[7]
    return gcm, rcm


hist_dict = {}
for f in hist_files:
    gcm, rcm = extract_gcm_rcm(f)
    hist_dict.setdefault((gcm, rcm), f)

ssp_dict = {}
for f in ssp_files:
    gcm, rcm = extract_gcm_rcm(f)
    ssp_dict.setdefault((gcm, rcm), f)

# -----------------------------------------------
# 2. Charger le tableau des modèles
# -----------------------------------------------

df = pd.read_csv(os.path.join(INPUT, "TRACC_pivot.csv"))

# -----------------------------------------------
# 3. Fonction pour déterminer la période
# -----------------------------------------------


def get_period(is_historical, pivot):
    if is_historical:
        return 1985, 2014

    start = pivot - 15
    end = pivot + 14
    return start, end


# -----------------------------------------------
# 4. BOUCLE CENTRALE : 1 LIGNE CSV = 1 MODÈLE
# -----------------------------------------------

datestart = time.time()

mods = []
resultats = {}
for RWL in ["2C", "2.7C", "4C"]:
    print("\n\n========================")
    print(f"\n===   RWL : {RWL}   ===")
    print("\n========================\n\n")

    HIST_temp, RCP8_temp = [], []

    for _, row in df.iterrows():

        gcm = row["GCM"]
        rcm = row["RCM"]
        pivot = row[RWL]

        model_key = f"{gcm}__{rcm}"
        print(f"\n=== MODÈLE : {model_key} ===")

        if pd.isna(pivot):
            pivot = 2085
            print(" - Pivot manquant → modèle ignoré ou set à 2085")
            # continue

        pivot = int(pivot)

        if pivot > 2086:
            pivot = 2085
            print(" - Pivot 4C > 2086 → modèle ignoré ou set à 2085")
            # continue

        hist_path = hist_dict.get((gcm, rcm), None)
        ssp_path = ssp_dict.get((gcm, rcm), None)

        if hist_path is None or ssp_path is None:
            print(f" - Fichiers manquants pour {model_key}")
            continue

        resultats[model_key] = {}

        # ----------- TRAITEMENT HISTORIQUE -----------
        print(f"  → HISTORIQUE : {hist_path}")

        if any(model_key in s for s in mods):
            print("---------{model_key} historique déjà réalisé")
        else:
            start, end = get_period(True, pivot)
            ds_hist = xr.open_dataset(os.path.join(PATH_DATA, hist_path))
            ds_hist_sel = ds_hist.sel(
                time=slice(f"{start}-01-01", f"{end}-12-31")
            )
            maximums_hist = ds_hist_sel.resample(time="1Y").max()

            rv, params = xr.apply_ufunc(
                RP_calcul_vectorized,
                maximums_hist[VAR],
                periods,
                input_core_dims=[["time"], ["periods"]],
                output_core_dims=[["periods"], ["gev_params"]],
                vectorize=True,
                dask="parallelized",
                output_dtypes=[float, float],
            )

            rv = rv.assign_coords(periods=periods)

            ds_RP = xr.Dataset({"return_levels": rv, "gev_params": params})

            HIST_temp.append(ds_RP)
            mods.append(model_key)

        # ----------- TRAITEMENT SSP370 -----------
        print(f"  → SSP370 : {ssp_path}")

        start, end = get_period(False, pivot)
        ds_ssp = xr.open_dataset(os.path.join(PATH_DATA, ssp_path))
        ds_ssp_sel = ds_ssp.sel(time=slice(f"{start}-01-01", f"{end}-12-31"))
        maximums_hist = ds_ssp_sel.resample(time="1Y").max()

        rv, params = xr.apply_ufunc(
            RP_calcul_vectorized,
            maximums_hist[VAR],
            periods,
            input_core_dims=[["time"], ["periods"]],
            output_core_dims=[["periods"], ["gev_params"]],
            vectorize=True,
            dask="parallelized",
            output_dtypes=[float, float],
        )

        rv = rv.assign_coords(periods=periods)
        ds_RP = xr.Dataset({"return_levels": rv, "gev_params": params})

        RCP8_temp.append(ds_ssp_sel)
        dateend = time.time()
        print(f"1 modèle - 1 RWL : {(dateend - datestart)/60} min")

    # fin boucle RWL
    if mods and not HIST_temp:
        file_name_hist = f"{VAR}_RP_hist_ref_median.nc"
        HIST = xr.concat(HIST_temp, dim="modele")
        HIST = HIST.median(dim="modele").to_netcdf(
            os.ath.join(OUTPUT, file_name_hist)
        )

        file_name_hist = f"{VAR}_RP_hist_ref_sup.nc"
        HIST = xr.concat(HIST_temp, dim="modele")
        HIST = HIST.quantile(0.95, dim="modele").to_netcdf(
            os.ath.join(OUTPUT, file_name_hist)
        )

        file_name_hist = f"{VAR}_RP_hist_ref_inf.nc"
        HIST = xr.concat(HIST_temp, dim="modele")
        HIST = HIST.quantile(0.05, dim="modele").to_netcdf(
            os.ath.join(OUTPUT, file_name_hist)
        )

    file_name_rcp8 = f"{VAR}_RP_ssp3_+{RWL}_median.nc"
    RCP8 = xr.concat(RCP8_temp, dim="modele")
    RCP8 = RCP8.median(dim="modele").to_netcdf(
        os.ath.join(OUTPUT, file_name_rcp8)
    )

    file_name_rcp8 = f"{VAR}_RP_ssp3_+{RWL}_sup.nc"
    RCP8 = xr.concat(RCP8_temp, dim="modele")
    RCP8 = RCP8.quantile(0.95, dim="modele").to_netcdf(
        os.ath.join(OUTPUT, file_name_rcp8)
    )

    file_name_rcp8 = f"{VAR}_RP_ssp3_+{RWL}_inf.nc"
    RCP8 = xr.concat(RCP8_temp, dim="modele")
    RCP8 = RCP8.quantile(0.05, dim="modele").to_netcdf(
        os.ath.join(OUTPUT, file_name_rcp8)
    )

    dateend = time.time()
    print(f"ALL modèles - 1 RWL : {(dateend - datestart)/60} min")

print("==" * 30)
print("        DONE       ")
dateend = time.time()
print(f"ALL modèles - ALL RWL : {(dateend - datestart)/60} min")
print("==" * 30)
