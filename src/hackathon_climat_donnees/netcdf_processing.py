"""
Created on Wed Dec  3 11:31:14 2025

@author: SamyKraiem
"""

import logging
import os
import time
import gc

import xarray as xr
import pandas as pd
import numpy as np
from scipy.stats import genextreme as gev

from hackathon_climat_donnees import INPUT, OUTPUT


logger = logging.getLogger(__name__)


# ----------------------------
# 1. Fonction GEV
# ----------------------------
def RP_calcul_vectorized(extremes, periods):
    ext = extremes[~np.isnan(extremes)]
    if ext.size < 5:
        return np.full(len(periods), np.nan), np.full(3, np.nan)
    fit = gev.fit(ext)
    return gev.ppf(1 - 1 / periods, *fit), np.array(fit)


# ----------------------------
# 2. Période
# ----------------------------
def get_period(is_historical, pivot):
    if is_historical:
        return 1985, 2014
    return pivot - 15, pivot + 14


# ----------------------------
# 3. Extraction GCM / RCM
# ----------------------------
def extract_gcm_rcm(path):
    parts = path.split("/")
    gcm = parts[5]
    rcm = parts[7]
    return gcm, rcm


def convert(data, var):
    if var == "prAdjust":
        data[var] = data[var] * 86400
    elif var == "tasmaxAdjust":
        data[var] = data[var] - 273.15
    return data


def process_netcdf_bunch():

    VAR = "tasmaxAdjust"
    periods = np.array([2, 5, 10, 20, 50, 100])
    HIST_DONE = False

    # ------------------------
    # 4.3 Listes fichiers
    # ------------------------
    with open(os.path.join(INPUT, "liste_ssp370_tasmax.txt")) as f:
        ssp_files = [l.strip() for l in f.readlines()]
    with open(os.path.join(INPUT, "liste_hist_tasmax.txt")) as f:
        hist_files = [l.strip() for l in f.readlines()]

    hist_dict = {extract_gcm_rcm(f): f for f in hist_files}
    ssp_dict = {extract_gcm_rcm(f): f for f in ssp_files}

    # ------------------------
    # 4.4 Tableau des modèles
    # ------------------------
    df = pd.read_csv(os.path.join(INPUT, "TRACC_pivot.csv"))

    # ------------------------
    # 4.5 Boucle RWL
    # ------------------------
    datestart = time.time()
    mods = []

    for _, row in df.iterrows():
        gcm = row["GCM"]
        rcm = row["RCM"]
        model_key = f"{gcm}__{rcm}"

        hist_path = hist_dict.get((gcm, rcm))
        ssp_path = ssp_dict.get((gcm, rcm))

        if hist_path is None or ssp_path is None:
            logger.warning(f"Fichiers manquants pour {model_key}")
            continue

        ds_hist = xr.open_dataset(os.path.join(INPUT, hist_path))
        ds_hist = convert(ds_hist, VAR)

        ds_ssp = xr.open_dataset(os.path.join(INPUT, ssp_path))
        ds_ssp = convert(ds_ssp, VAR)

        for RWL in ["2C", "2.7C", "4C"]:
            logger.info(f"=== RWL : {RWL} ===")
            pivot = row[RWL]
            if pd.isna(pivot):
                pivot = 2085
            else:
                pivot = min(int(pivot), 2085)

            # ------------------------
            # Historique
            # ------------------------
            if model_key not in mods and not HIST_DONE:
                ds_hist_sel = ds_hist.sel(
                    time=slice("1985-01-01", "2014-12-31")
                )
                maximums_hist = ds_hist_sel.resample(time="1YE").max(
                    skipna=True
                )
                # maximums_hist = maximums_hist.chunk({"time": -1})

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

                ds_RP.to_netcdf(
                    os.path.join(OUTPUT, f"{VAR}_RP_hist_{model_key}.nc")
                )

                mods.append(model_key)
                del ds_hist_sel, maximums_hist, rv, params, ds_RP
                gc.collect()
                logger.info(f"Historique traité pour {model_key}")
            else:
                logger.info(f"{model_key} historique déjà traité")

            # ------------------------
            # SSP370
            # ------------------------
            start, end = get_period(False, pivot)

            ds_ssp_sel = ds_ssp.sel(
                time=slice(f"{start}-01-01", f"{end}-12-31")
            )
            maximums_ssp = ds_ssp_sel.resample(time="1YE").max(skipna=True)
            # maximums_ssp = maximums_ssp.chunk({"time": -1})

            rv, params = xr.apply_ufunc(
                RP_calcul_vectorized,
                maximums_ssp[VAR],
                periods,
                input_core_dims=[["time"], ["periods"]],
                output_core_dims=[["periods"], ["gev_params"]],
                vectorize=True,
                dask="parallelized",
                output_dtypes=[float, float],
            )
            rv = rv.assign_coords(periods=periods)
            ds_RP = xr.Dataset({"return_levels": rv, "gev_params": params})

            ds_RP.to_netcdf(
                os.path.join(OUTPUT, f"{VAR}_RP_ssp3_{model_key}_+{RWL}.nc")
            )

            del ds_ssp_sel, maximums_ssp, rv, params, ds_RP
            gc.collect()

            logger.info(
                f"RWL {RWL} terminée ({(time.time()-datestart)/60:.2f} min)"
            )

    logger.info(f"Temps total : {(time.time()-datestart)/60:.2f} min")

    # ------------------------
    # 4.6 Reconstruction médianes / quantiles finales
    # ------------------------

    def compute_final_statistics(
        output_dir, input_path, var, RWL_list=["2C", "2.7C", "4C"]
    ):

        def load_concat_reduce(files, out_prefix):
            """Charge chaque fichier avec open_dataset puis concatène."""
            if not files:
                return

            logger.info(f"  - {len(files)} fichiers trouvés")

            datasets = []
            for f in files:
                ds = xr.open_dataset(f)
                if "gev_params" in ds:
                    ds = ds.drop_vars("gev_params")

                datasets.append(ds)

            ds_all = xr.concat(
                datasets, dim="modele", compat="override", coords="minimal"
            )

            ds_all.median(dim="modele").to_netcdf(
                os.path.join(input_path, f"{out_prefix}_median.nc")
            )
            ds_all.quantile(0.95, dim="modele").to_netcdf(
                os.path.join(input_path, f"{out_prefix}_sup.nc")
            )
            ds_all.quantile(0.05, dim="modele").to_netcdf(
                os.path.join(input_path, f"{out_prefix}_inf.nc")
            )

        for RWL in RWL_list:
            logger.info(
                f"\nReconstruction médianes / quantiles pour RWL {RWL}"
            )

            # # --- Historique ---
            hist_files = [
                os.path.join(output_dir, f)
                for f in os.listdir(output_dir)
                if f.startswith(f"{var}_RP_hist_") and f.endswith(".nc")
            ]
            load_concat_reduce(hist_files, f"{var}_RP_hist_ref")

            # --- SSP370 ---
            ssp_files = [
                os.path.join(output_dir, f)
                for f in os.listdir(output_dir)
                if f.startswith(f"{var}_RP_ssp3_") and f.endswith(f"+{RWL}.nc")
            ]
            load_concat_reduce(ssp_files, f"{var}_RP_ssp3_+{RWL}")

        logger.info("\nReconstruction terminée.")

    compute_final_statistics("out", OUTPUT, VAR, RWL_list=["2C", "2.7C", "4C"])


if __name__ == "__main__":
    process_netcdf_bunch()
