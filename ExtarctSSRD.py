import xarray as xr
import os
import numpy as np
import pandas as pd
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor

# === Global parameters ===
year = 2024
months = [f"{i:02d}" for i in range(1, 13)]
ssrd_root = f"G:/GlobalFPV/ERA5Land/licd&ssrd"
output_root = f"G:/GlobalFPV/LakeArea/ssrd"

# === Load the spatial mask and FID index, only once ===
HasLake = np.load('./data/HasLake.npy')   # shape: (lat, lon), constructed with ascending latitude
Index = np.load('./data/GridID.npy')      # same shape
rows, cols = np.where(HasLake == 1)
fids = Index[rows, cols]

# === Monthly processing function ===
def process_month(month):
    print(f"\n processing {year}-{month} ...")


    ssrd_path = os.path.join(ssrd_root, f"ERA5Land_{year}_{month}_SSRD_ICE.nc")
    ds_ssrd = xr.open_dataset(ssrd_path)
    ssrd_all = ds_ssrd['ssrd']

    # Extract latitude and longitude arrays.
    lats = ssrd_all['latitude'].values
    lons = ssrd_all['longitude'].values
    lat2d, lon2d = np.meshgrid(lats, lons, indexing='ij')  # Correct order, no transpose needed.

    # Flip the row index, because Fishnet uses ascending latitude while NetCDF uses descending latitude.
    # see more in "initFishnet.py"
    nlat = ssrd_all.shape[1] if lats.ndim == 1 else lats.shape[0]
    rows_flipped = nlat - 1 - rows

    # Output folder.
    ssrd_output_folder = os.path.join(output_root, f"{year}_{month}")
    os.makedirs(ssrd_output_folder, exist_ok=True)

    # Extract cumulative values at 00:00.
    valid_times = ssrd_all['valid_time'].values
    for i in tqdm(range(len(valid_times)), desc=f" {month}月 SSRD 00:00"):

        time = pd.to_datetime(str(valid_times[i]))
        if time.hour != 0:
            continue

        date_str = (time - pd.Timedelta(days=1)).strftime("%Y_%m_%d")
        save_path = os.path.join(ssrd_output_folder, f"{date_str}.parquet")
        if os.path.exists(save_path):
            continue

        ssrd_hour = ssrd_all.isel(valid_time=i)
        ssrd_vals = ssrd_hour.values[rows_flipped, cols]
        lat_vals = lat2d[rows_flipped, cols]
        lon_vals = lon2d[rows_flipped, cols]

        df_ssrd = pd.DataFrame({
            'FID': fids,
            'date': date_str,
            'ssrd_sum': ssrd_vals,
            'lat': lat_vals,
            'lon': lon_vals
        })

        df_ssrd.to_parquet(save_path, compression='snappy')
        tqdm.write(f" SSRD saved: {save_path}")


# === Run with multiprocessing ===
if __name__ == '__main__':
    with ProcessPoolExecutor(max_workers=4) as executor:
        executor.map(process_month, months)





# %%
import xarray as xr
import os
import numpy as np
import pandas as pd
from tqdm import tqdm

ssrd_file = r"G:/GlobalFPV/ERA5Land/licd&ssrd/ssrd_20250101.nc"
output_root = r"G:/GlobalFPV/LakeArea/ssrd"
os.makedirs(output_root, exist_ok=True)

# === Load the spatial mask and FID index ===
HasLake = np.load('./data/HasLake.npy')
Index = np.load('./data/GridID.npy')
rows, cols = np.where(HasLake == 1)
fids = Index[rows, cols]

def process_single_file(ssrd_file):
    ds_ssrd = xr.open_dataset(ssrd_file)
    ssrd_all = ds_ssrd['ssrd']

    # Extract latitude and longitude arrays.
    lats = ssrd_all['latitude'].values
    lons = ssrd_all['longitude'].values
    lat2d, lon2d = np.meshgrid(lats, lons, indexing='ij')

    # Get the latitude dimension size of the NetCDF file.
    nlat = ssrd_all.shape[1] if lats.ndim == 1 else lats.shape[0]
    rows_flipped = nlat - 1 - rows

    # Extract the time dimension.
    valid_times = ssrd_all['valid_time'].values
    for i in tqdm(range(len(valid_times)), desc=f" SSRD 00:00"):
        time = pd.to_datetime(str(valid_times[i]))
        if time.hour != 0:
            continue

        date_str = (time - pd.Timedelta(days=1)).strftime("%Y_%m_%d")
        save_path = os.path.join(output_root, f"{date_str}.parquet")
        if os.path.exists(save_path):
            continue

        ssrd_hour = ssrd_all.isel(valid_time=i)
        ssrd_vals = ssrd_hour.values[rows_flipped, cols]
        lat_vals = lat2d[rows_flipped, cols]
        lon_vals = lon2d[rows_flipped, cols]

        df_ssrd = pd.DataFrame({
            'GridID': fids,
            'date': date_str,
            'ssrd_sum': ssrd_vals,
            'lat': lat_vals,
            'lon': lon_vals
        })

        df_ssrd.to_parquet(save_path, compression='snappy')
        tqdm.write(f" SSRD saved: {save_path}")


process_single_file(ssrd_file)

# %%