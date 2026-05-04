import pandas as pd
import numpy as np
import os
from tqdm import tqdm

# Threshold setting.
LICD_THRESHOLD = 0.1  # Unit: meters, 10 cm.
year = 2024
licd_dir = r'G:\GlobalFPV\LakeArea\licd'
output_csv = './CalStudyArea/freq/licd_freq.csv'

def compute_licd_freq_from_hourly():
    stats = {}
    all_files = sorted([f for f in os.listdir(licd_dir) if f.endswith('.parquet')])

    for fname in tqdm(all_files, desc=f"Statistics for ice thickness frequency in {year}"):
        date = pd.to_datetime(fname.replace('.parquet', ''), format='%Y-%m-%d')
        if date.year != year:
            continue

        file_path = os.path.join(licd_dir, fname)
        df = pd.read_parquet(file_path)

        # Ensure required fields exist.
        if not {'GridID', 'licd'}.issubset(df.columns):
            continue

        licd_mask = df['licd'].notna()
        licd_valid = df[licd_mask].groupby('GridID').size()
        licd_count = df[licd_mask & (df['licd'] >= LICD_THRESHOLD)].groupby('GridID').size()

        all_ids = set(licd_valid.index).union(licd_count.index)
        for gid in all_ids:
            if gid not in stats:
                stats[gid] = {'licd_count': 0, 'licd_valid': 0}
            stats[gid]['licd_count'] += licd_count.get(gid, 0)
            stats[gid]['licd_valid'] += licd_valid.get(gid, 0)

    # Summarize results.
    df_out = pd.DataFrame.from_dict(stats, orient='index').reset_index().rename(columns={'index': 'GridID'})
    df_out['licd_freq'] = df_out['licd_count'] / df_out['licd_valid']
    df_out.to_csv(output_csv, index=False)
    print(f"Ice thickness frequency statistics saved: {output_csv}")

# Execute.
if __name__ == "__main__":
    compute_licd_freq_from_hourly()