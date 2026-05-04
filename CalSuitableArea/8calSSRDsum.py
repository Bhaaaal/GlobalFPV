import pandas as pd
import numpy as np
import os
from tqdm import tqdm

# Set the threshold, unit: kWh/m².
SSRD_THRESHOLD_KWH = 847.0
J_TO_KWH = 1 / 3.6e6  # Convert J/m² to kWh/m².

year = 2024
ssrd_dir = r'G:\GlobalFPV\LakeArea\ssrd'
output_csv = './CalStudyArea/freq/ssrd_sum.csv'

def compute_ssrd_sum_from_hourly():
    stats = {}

    # Traverse all subdirectories and collect all .parquet files.
    all_files = []
    for root, dirs, files in os.walk(ssrd_dir):
        for file in files:
            if file.endswith('.parquet'):
                all_files.append(os.path.join(root, file))

    all_files = sorted(all_files)

    for file_path in tqdm(all_files, desc=f"SSRD accumulation statistics for {year}"):
        fname = os.path.basename(file_path)
        try:
            date = pd.to_datetime(fname.replace('.parquet', ''), format='%Y_%m_%d')
        except Exception as e:
            print(f"Unable to parse the date from file name: {fname}, skipped. Error message: {e}")
            continue

        if date.year != year:
            continue

        df = pd.read_parquet(file_path)

        if not {'FID', 'ssrd_sum'}.issubset(df.columns):
            print(f"Missing required fields, skipped: {fname}")
            continue

        df_valid = df[df['ssrd_sum'].notna()]
        ssrd_agg = df_valid.groupby('FID')['ssrd_sum'].sum()

        for gid, val in ssrd_agg.items():
            stats[gid] = stats.get(gid, 0.0) + val

    # Summarize results.
    df_out = pd.DataFrame(list(stats.items()), columns=['FID', 'ssrd_sum_j'])
    df_out['ssrd_sum_kwh'] = df_out['ssrd_sum_j'] * J_TO_KWH
    df_out['above_threshold'] = df_out['ssrd_sum_kwh'] > SSRD_THRESHOLD_KWH

    os.makedirs(os.path.dirname(output_csv), exist_ok=True)
    df_out.to_csv(output_csv, index=False)
    print(f"SSRD accumulation statistics saved: {output_csv}")

# Execute.
if __name__ == "__main__":
    compute_ssrd_sum_from_hourly()