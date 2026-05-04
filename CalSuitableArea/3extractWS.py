import xarray as xr
import pandas as pd
import numpy as np
import os
import gc
from datetime import datetime, timedelta
from multiprocessing import get_context
from tqdm import tqdm

def extract_day_ws(start_index, date_str, nc_file, directory_path, rows_flipped, cols, fid_values):
    output_file = f'{directory_path}{date_str}.parquet'
    if os.path.exists(output_file):
        print(f"Already exists, skipping: {output_file}")
        return

    try:
        with xr.open_dataset(nc_file) as ds:
            u10_day = ds['u10'].isel(valid_time=slice(start_index, start_index + 24)).values[:, rows_flipped, cols]
            v10_day = ds['v10'].isel(valid_time=slice(start_index, start_index + 24)).values[:, rows_flipped, cols]
            ws_day = np.sqrt(u10_day**2 + v10_day**2)
    except Exception as e:
        print(f"Error @ {date_str}: {e}")
        return

    base_dt = datetime.strptime(date_str, "%Y-%m-%d")
    time_list = [base_dt + timedelta(hours=h) for h in range(24)]
    time_list = np.tile(time_list, len(fid_values))

    fid_repeated = np.repeat(fid_values, 24)
    ws_flat = ws_day.T.flatten()

    df_day = pd.DataFrame({
        'GridID': fid_repeated,
        'time': time_list,
        'ws': ws_flat
    })

    df_day.to_parquet(output_file, compression='snappy')
    print(f"Write completed: {output_file}")

    del u10_day, v10_day, ws_day, ws_flat, df_day
    gc.collect()

def extract_ws_by_day(YEAR, MONTH, processes=8):
    directory_path = f'G:/GlobalFPV/LakeArea/ws/'
    os.makedirs(directory_path, exist_ok=True)

    nc_file = f'G:/GlobalFPV/ERA5Land/ws&tp/ERA5Land_{YEAR}_{MONTH}.nc'

    HasLake = np.load('./data/HasLake.npy')
    Index = np.load('./data/GridID.npy')
    rows, cols = np.where(HasLake == 1)
    fid_values = Index[rows, cols]

    with xr.open_dataset(nc_file) as ds:
        lats = ds['latitude'].values
        nlat = lats.shape[0] if lats.ndim == 1 else lats.shape[0]
        rows_flipped = nlat - 1 - rows
        valid_times = ds['valid_time'].values

    total_days = len(valid_times) // 24
    base_date = datetime.strptime(f'{YEAR}-{MONTH}-01', '%Y-%m-%d')

    tasks = []
    for i in range(total_days):
        date_str = (base_date + timedelta(days=i)).strftime('%Y-%m-%d')
        tasks.append((i * 24, date_str, nc_file, directory_path, rows_flipped, cols, fid_values))

    print(f"Starting parallel ws extraction for {YEAR}-{MONTH}, total days: {total_days}, processes: {processes}")

    with get_context("spawn").Pool(processes=processes) as pool:
        list(tqdm(pool.starmap(extract_day_ws, tasks), total=len(tasks)))

    print(f"{YEAR}-{MONTH} extraction completed")

if __name__ == '__main__':
    YEAR = 2024
    months = [f"{m:02d}" for m in range(1, 13)]
    processes = 8

    for month in months:
        try:
            extract_ws_by_day(YEAR, month, processes=processes)
        except Exception as e:
            print(f"Skipping {YEAR}-{month}, error message: {e}")