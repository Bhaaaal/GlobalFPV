import pandas as pd
import numpy as np
import os
from tqdm import tqdm

# Threshold setting.
WS_THRESHOLD = 13.9 #m/s
year = 2024
ws_dir = r'G:\GlobalFPV\LakeArea\ws'
output_csv = './CalStudyArea/freq/ws_freq.csv'

def compute_ws_freq_from_hourly():
    stats = {}
    all_files = sorted([f for f in os.listdir(ws_dir) if f.endswith('.parquet')])

    for fname in tqdm(all_files, desc=f"Statistics for wind speed frequency in {year}"):
        date = pd.to_datetime(fname.replace('.parquet', ''), format='%Y-%m-%d')
        if date.year != year:
            continue

        file_path = os.path.join(ws_dir, fname)
        df = pd.read_parquet(file_path)

        # Ensure required fields exist.
        if not {'GridID', 'ws'}.issubset(df.columns):
            continue

        ws_mask = df['ws'].notna()
        ws_valid = df[ws_mask].groupby('GridID').size()
        ws_count = df[ws_mask & (df['ws'] >= WS_THRESHOLD)].groupby('GridID').size()

        all_ids = set(ws_valid.index).union(ws_count.index)
        for gid in all_ids:
            if gid not in stats:
                stats[gid] = {'ws_count': 0, 'ws_valid': 0}
            stats[gid]['ws_count'] += ws_count.get(gid, 0)
            stats[gid]['ws_valid'] += ws_valid.get(gid, 0)

    # Summarize results.
    df_out = pd.DataFrame.from_dict(stats, orient='index').reset_index().rename(columns={'index': 'GridID'})
    df_out['ws_freq'] = df_out['ws_count'] / df_out['ws_valid']
    df_out.to_csv(output_csv, index=False)
    print(f"Wind speed frequency statistics saved: {output_csv}")

# Execute.
if __name__ == "__main__":
    compute_ws_freq_from_hourly()