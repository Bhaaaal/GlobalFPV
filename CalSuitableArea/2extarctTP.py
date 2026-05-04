# Extract precipitation data. Precipitation is cumulative, but all values still need to be extracted first and then differenced.
# The data extracted into the tp folder has not been differenced. The extractTP2 file processes 20250101 00:00, which corresponds to 2024.12.31 24:00.
import xarray as xr
import pandas as pd
import numpy as np
import os
import gc
from datetime import datetime, timedelta
from multiprocessing import get_context
from tqdm import tqdm

def extract_day_tp(start_index, date_str, nc_file, directory_path, rows_flipped, cols, fid_values):
    output_file = f'{directory_path}{date_str}.parquet'
    if os.path.exists(output_file):
        print(f"Already exists, skipping: {output_file}")
        return

    try:
        with xr.open_dataset(nc_file) as ds:
            tp_day = ds['tp'].isel(valid_time=slice(start_index, start_index + 24)).values[:, rows_flipped, cols]
    except Exception as e:
        print(f"Error @ {date_str}: {e}")
        return

    base_dt = datetime.strptime(date_str, "%Y-%m-%d")
    time_list = [base_dt + timedelta(hours=h) for h in range(24)]
    time_list = np.tile(time_list, len(fid_values))

    fid_repeated = np.repeat(fid_values, 24)
    tp_flat = tp_day.T.flatten()

    df_day = pd.DataFrame({
        'GridID': fid_repeated,
        'time': time_list,
        'tp': tp_flat
    })

    df_day.to_parquet(output_file, compression='snappy')
    print(f"Write completed: {output_file}")

    del tp_day, tp_flat, df_day
    gc.collect()

def extract_tp_by_day(YEAR, MONTH, processes=8):
    directory_path = f'G:/GlobalFPV/LakeArea/tp/'
    os.makedirs(directory_path, exist_ok=True)

    # f"G:/GlobalFPV/ERA5Land/tp&ssrd"
    # "G:\GlobalFPV\ERA5Land\tp&ssrd\ERA5Land_2024_01_SSRD_ICE.nc"
    nc_file = f'G:/GlobalFPV/ERA5Land/ws&tp/ERA5Land_{YEAR}_{MONTH}.nc'

    # === Load mask and index ===
    HasLake = np.load('./data/HasLake.npy')
    Index = np.load('./data/GridID.npy')
    rows, cols = np.where(HasLake == 1)
    fid_values = Index[rows, cols]

    # === Get latitude dimension length and flip row indices ===
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

    print(f"Starting parallel tp extraction for {YEAR}-{MONTH}, total days: {total_days}, processes: {processes}")

    with get_context("spawn").Pool(processes=processes) as pool:
        list(tqdm(pool.starmap(extract_day_tp, tasks), total=len(tasks)))

    print(f"{YEAR}-{MONTH} extraction completed")
    
def extract_tp_single_time(nc_file):
    # === Extract the date, e.g., tp_20250101.nc → 2025-01-01 ===
    date_part = os.path.basename(nc_file).split('_')[1].split('.')[0]
    date_obj = datetime.strptime(date_part, "%Y%m%d")  # datetime object
    date_str = date_obj.strftime("%Y-%m-%d")  # string format for the file name
    output_file = f'G:/GlobalFPV/LakeArea/tp/{date_str}.parquet'
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    if os.path.exists(output_file):
        print(f"Already exists, skipping: {output_file}")
        return

    try:
        # === Load spatial mask ===
        HasLake = np.load('./data/HasLake.npy')
        Index = np.load('./data/GridID.npy')
        rows, cols = np.where(HasLake == 1)
        fid_values = Index[rows, cols]

        with xr.open_dataset(nc_file) as ds:
            lats = ds['latitude'].values
            nlat = lats.shape[0]
            rows_flipped = nlat - 1 - rows

            # Note: there is only one time point.
            tp = ds['tp'].isel(valid_time=0).values[rows_flipped, cols]

        # === Construct the DataFrame, consistently use the 'time' column, and set it as datetime type ===
        df = pd.DataFrame({
            'GridID': fid_values,
            'time': [date_obj] * len(fid_values),  # Use the datetime object for each row.
            'tp': tp
        })

        df.to_parquet(output_file, compression='snappy')
        print(f"Extraction completed: {output_file}")

        del df, tp
        gc.collect()

    except Exception as e:
        print(f"Error: {nc_file} | {e}")

if __name__ == '__main__':
    YEAR = 2024
    months = [f"{m:02d}" for m in range(1, 13)]
    processes = 8

    for month in months:
        try:
            extract_tp_by_day(YEAR, month, processes=processes)
        except Exception as e:
            print(f"Skipping {YEAR}-{month}, error message: {e}")
            
    # Since ERA5-Land 00:00 represents 24:00 of the previous day,
    # the complete 2024 series also needs the 20250101 data to be supplemented separately.
    # Used to process 20250101 separately.
    
    nc_file = "G:/GlobalFPV/ERA5Land/ws&tp/tp_20250101.nc"
    extract_tp_single_time(nc_file)