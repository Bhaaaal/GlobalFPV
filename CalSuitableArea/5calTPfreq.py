import pandas as pd
import numpy as np
import os
from tqdm import tqdm


# Number of valid hours, tp_valid.

# Number of extreme precipitation hours, tp_hourly_mm >= 50 mm, namely tp_count.

# Final frequency calculation: tp_freq = tp_count / tp_valid.

# If a GridID has no valid data, it will not appear in the final frequency file. The same applies to other variables.

# Set paths and threshold.
TP_THRESHOLD_MM = 50.0 #mm
year = 2024
hourly_dir = r'G:\GlobalFPV\LakeArea\tp_hourly'
output_csv = './CalStudyArea/freq/tp_freq.csv'

def compute_tp_freq_from_hourly():
    stats = {}
    all_files = sorted([f for f in os.listdir(hourly_dir) if f.endswith('.parquet')])

    for fname in tqdm(all_files, desc=f"Statistics for precipitation frequency in {year}"):
        date = pd.to_datetime(fname.replace('.parquet', ''), format='%Y-%m-%d')
        if date.year != year:
            continue

        file_path = os.path.join(hourly_dir, fname)
        df = pd.read_parquet(file_path)

        # Ensure required fields exist.
        if not {'GridID', 'tp_hourly_mm'}.issubset(df.columns):
            continue

        # Count valid hours.
        tp_mask = df['tp_hourly_mm'].notna()
        tp_valid = df[tp_mask].groupby('GridID').size()
        tp_count = df[tp_mask & (df['tp_hourly_mm'] >= TP_THRESHOLD_MM)].groupby('GridID').size()

        all_ids = set(tp_valid.index).union(tp_count.index)
        for gid in all_ids:
            if gid not in stats:
                stats[gid] = {'tp_count': 0, 'tp_valid': 0}
            stats[gid]['tp_count'] += tp_count.get(gid, 0)
            stats[gid]['tp_valid'] += tp_valid.get(gid, 0)

    # Summarize as a DataFrame.
    df_out = pd.DataFrame.from_dict(stats, orient='index').reset_index().rename(columns={'index': 'GridID'})
    df_out['tp_freq'] = df_out['tp_count'] / df_out['tp_valid']
    df_out.to_csv(output_csv, index=False)
    print(f"Precipitation frequency statistics saved: {output_csv}")

if __name__ == "__main__":
    compute_tp_freq_from_hourly()