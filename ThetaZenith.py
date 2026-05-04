# calculate PV panel tilt angle and zenith.

# %%
# Determine the time zone based on lon and lat.
from timezonefinder import TimezoneFinder
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from functools import partial

# === Read data ===
df = pd.read_parquet(r"G:\GlobalFPV\LakeArea\ssrd\2024_01\2024_01_01.parquet")
df['lon'] = df['lon'].apply(lambda x: x - 360 if x > 180 else x)

# === Remove duplicate coordinates ===
unique_coords = df[['lat', 'lon']].drop_duplicates().copy()
coord_list = list(zip(unique_coords['lat'], unique_coords['lon']))

# === Initialize TimezoneFinder ===
tf = TimezoneFinder(in_memory=True)

def query_timezone(tf, lat, lon):
    try:
        return tf.timezone_at(lat=lat, lng=lon)
    except:
        return None

query_func = partial(query_timezone, tf)

# === Parallel processing ===
with ThreadPoolExecutor(max_workers=-1) as executor:
    timezones = list(executor.map(lambda x: query_func(*x), coord_list))

unique_coords['timezone'] = timezones



#%%
# Calculate the PV panel tilt angle.
# The calculation code is from:
# https://github.com/renewables-ninja/gsee/blob/main/src/gsee/pv.py
def optimal_tilt(lat):

    lat = abs(lat)
    if lat <= 25:
        return lat * 0.87
    elif lat <= 50:
        return (lat * 0.76) + 3.1
    else:  # lat > 50
        # raise NotImplementedError('Not implemented for latitudes beyond 50.')
        return 40  # Simply use 40 degrees above lat 50
    
unique_coords['Theta'] = unique_coords['lat'].apply(optimal_tilt)    
unique_coords.to_csv('./data/TimeZone&Theta.csv',index=False)


# %%
import os
import pandas as pd
from pathlib import Path
from tqdm import tqdm
from pvlib.location import Location
from pvlib.solarposition import get_solarposition
from zoneinfo import ZoneInfo
from concurrent.futures import ProcessPoolExecutor
import multiprocessing

# Set the current working directory.
os.chdir('/root/FPV')

# === Global parameters ===
input_dir = './ssrd'
output_dir = './ssrd_zenith'
os.makedirs(output_dir, exist_ok=True)

# Load the time zone and tilt angle table.
TimeZoneZenith = pd.read_csv('./data/TimeZone&Theta.csv')

# === Solar angle calculation function, processed serially within each process ===
def compute_zenith_row(row):
    try:
        tz = row['timezone']
        if pd.isna(tz):
            return None
        loc = Location(latitude=row['lat'], longitude=row['lon'], tz=tz)
        tzinfo = ZoneInfo(tz)
        local_date = pd.to_datetime(row['utc_time']).date()
        local_noon = pd.Timestamp(f"{local_date} 12:00:00").tz_localize(tzinfo)
        solar_pos = loc.get_solarposition(times=[local_noon])
        return solar_pos['zenith'].iloc[0]
    except:
        return None

# === Process a single file ===
def process_parquet_file(file_path):
    file_name = Path(file_path).name
    save_path = os.path.join(output_dir, file_name)

    if os.path.exists(save_path):
        print(f"exist: {file_name}")
        return

    print(f"processing: {file_name}")
    df = pd.read_parquet(file_path)
    #Process longitude and convert it to the -180 to 180 range.
    df['lon'] = df['lon'].apply(lambda x: x - 360 if x > 180 else x)


    #Round lon and lat
    df['lat'] = df['lat'].round(4)
    df['lon'] = df['lon'].round(4)
    TimeZoneZenith['lat'] = TimeZoneZenith['lat'].round(4)
    TimeZoneZenith['lon'] = TimeZoneZenith['lon'].round(4)


    # Merge time zone and tilt angle information.
    df = df.merge(TimeZoneZenith, on=['lat', 'lon'], how='left')
        
    df['utc_time'] = pd.to_datetime(df['date'], format='%Y_%m_%d')

    # Calculate zenith serially.
    df['zenith'] = [compute_zenith_row(row) for _, row in tqdm(df.iterrows(), total=len(df), desc=f"{file_name}")]

    # Save results.
    df.to_parquet(save_path, index=False, compression='snappy')
    print(f"保存完成: {save_path}")


def main():
    # Recursively find all .parquet files.
    all_files = [
        os.path.join(root, file)
        for root, _, files in os.walk(input_dir)
        for file in files if file.endswith('.parquet')
    ]

    max_workers = -1

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        list(tqdm(executor.map(process_parquet_file, all_files), total=len(all_files), desc="processing"))

if __name__ == '__main__':
    main()