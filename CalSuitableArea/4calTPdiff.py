#%%
import pandas as pd
import numpy as np
import os
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timedelta

# Parameter settings
tp_dir = r'G:\GlobalFPV\LakeArea\tp'
daily_out_dir = r'G:\GlobalFPV\LakeArea\tp_hourly'
os.makedirs(daily_out_dir, exist_ok=True)

def compute_tp_hourly_mm(df_day: pd.DataFrame, df_next_day: pd.DataFrame) -> pd.DataFrame:
    df = df_day[df_day['time'].dt.hour != 0].copy()  # Remove 00:00

    # Append 00:00 from the next day.
    if not df_next_day.empty:
        df_next = df_next_day[df_next_day['time'].dt.hour == 0]
        df = pd.concat([df, df_next], ignore_index=True)

    df = df.sort_values(['GridID', 'time'])

    # Difference, unit: m.
    df['tp_hourly_mm'] = df.groupby('GridID')['tp'].diff()

    # Use the tp value directly for the first hour.
    first_idx = df.groupby('GridID').head(1).index
    df.loc[first_idx, 'tp_hourly_mm'] = df.loc[first_idx, 'tp']

    # Convert the unit to mm.
    df['tp_hourly_mm'] *= 1000

    return df


def process_and_save_day(date_str):
    fname = f"{date_str}.parquet"
    save_path = os.path.join(daily_out_dir, fname)

    if os.path.exists(save_path):
        return f"Already exists, skipping: {fname}"

    file_path = os.path.join(tp_dir, fname)
    if not os.path.exists(file_path):
        return f"Missing: {fname}"

    try:
        # Read the current day.
        df_day = pd.read_parquet(file_path)

        # Read the next day to obtain 00:00.
        next_date = datetime.strptime(date_str, "%Y-%m-%d") + timedelta(days=1)
        next_fname = f"{next_date.strftime('%Y-%m-%d')}.parquet"
        next_path = os.path.join(tp_dir, next_fname)
        if os.path.exists(next_path):
            df_next_day = pd.read_parquet(next_path)
        else:
            df_next_day = pd.DataFrame(columns=['GridID', 'time', 'tp'])  # Empty DataFrame.

        df_out = compute_tp_hourly_mm(df_day, df_next_day)
        df_out.to_parquet(save_path, index=False)
        return f"Completed: {fname}"

    except Exception as e:
        return f"Error @ {fname}: {e}"


# Generate the date list for the full year of 2024.
date_list = [(datetime(2024, 1, 1) + timedelta(days=i)).strftime('%Y-%m-%d') for i in range(366)]

def sequential_process_tp_2024():
    results = []
    for date_str in tqdm(date_list, desc="Sequentially saving daily tp differences"):
        result = process_and_save_day(date_str)
        results.append(result)
        if result.startswith("Error"):
            print(result)


# Multiprocessing.
def parallel_process_tp_2024():
    with ProcessPoolExecutor(max_workers=8) as executor:
        results = list(tqdm(executor.map(process_and_save_day, date_list), total=len(date_list), desc="Saving daily tp differences"))
    for r in results:
        if r.startswith("Error"):
            print(r)

# Execute the main program.
if __name__ == "__main__":
    print("Start processing...")
    print(date_list)
    parallel_process_tp_2024()