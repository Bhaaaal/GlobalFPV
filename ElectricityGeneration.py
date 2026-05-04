# Used to calculate power generation.
# Calculates both the scenario with CoolingEffect
# and the hypothetical scenario without CoolingEffect.


#%%
# Part1 Estimate the scenario without water-body cooling effect (GPV Scenario)
# Part1.1 Construct LakeRegion and identify neighboring land areas
# see more in paper Method

# Label lake-covered areas and pure land areas.
import geopandas as gpd
import numpy as np

# === Load the original grid and HasLand mask ===
fishnet = gpd.read_file('./data/Fishnet_180_HasLake.shp')
HasLand = np.load('./data/HasLand.npy')  # shape: (nrows, ncols)

# === Add the IsLand field ===
fishnet['IsLand'] = 0
for idx, row in fishnet.iterrows():
    i = row['row']
    j = row['col']
    if 0 <= i < HasLand.shape[0] and 0 <= j < HasLand.shape[1]:
        fishnet.at[idx, 'IsLand'] = int(HasLand[i, j])
def classify_type(row):
    if row['HasLake'] == 1:
        return 'lake'
    elif row['IsLand'] == 1:
        return 'land'
    else:
        return None

fishnet['Type'] = fishnet.apply(classify_type, axis=1)

subset = fishnet[fishnet['Type'].notnull()]
subset.to_file('./data/Fishnet_Land_Lake.shp')

# Convert the shapefile to a matrix and calculate connectivity.
# Merge adjacent lake-covered cells into a LakeRegion (Paper Extended Fig2).

import geopandas as gpd
import numpy as np
from scipy.ndimage import label
# === Load the original grid and HasLand mask ===
fishnet = gpd.read_file('./data/Fishnet_180_HasLake.shp')
HasLand = np.load('./data/HasLand.npy')  # shape: (nrows, ncols)

# === Add the IsLand field ===
fishnet['IsLand'] = 0
for idx, row in fishnet.iterrows():
    i = row['row']
    j = row['col']
    if 0 <= i < HasLand.shape[0] and 0 <= j < HasLand.shape[1]:
        fishnet.at[idx, 'IsLand'] = int(HasLand[i, j])
def classify_type(row):
    if row['HasLake'] == 1:
        return 'lake'
    elif row['IsLand'] == 1:
        return 'land'
    else:
        return None  # Do not output other types.

fishnet['Type'] = fishnet.apply(classify_type, axis=1)
nrows = fishnet['row'].max() + 1
ncols = fishnet['col'].max() + 1

# === Construct the lake-type mask matrix ===
lake_mask = np.zeros((nrows, ncols), dtype=bool)
for _, row in fishnet.iterrows():
    i, j = row['row'], row['col']
    if row['Type'] == 'lake':
        lake_mask[i, j] = True

# === Label connected regions using 8-neighbor connectivity ===
structure = np.ones((3, 3), dtype=int)
labeled, num_lakes = label(lake_mask, structure=structure)

# === Write back to fishnet ===
fishnet['LakeRegion'] = -1
for idx, row in fishnet.iterrows():
    i, j = row['row'], row['col']
    if lake_mask[i, j]:
        fishnet.at[idx, 'LakeRegion'] = int(labeled[i, j])

# === Keep only lake blocks and export ===
lake_blocks = fishnet[fishnet['LakeRegion'] != -1].copy()
lake_blocks.to_file('./data/Fishnet_Lake_Region.shp')

# Identify edge cells for each LakeRegion.
# Use a set for recording.
import geopandas as gpd
import numpy as np
from collections import defaultdict
fishnet = gpd.read_file('./data/Fishnet_Land_Lake.shp')         # Contains all lake and land cells.
lake_blocks = gpd.read_file('./data/Fishnet_Lake_Region.shp')   # Contains lake cells with LakeRegion.

# === Extract GridID and LakeRegion fields from lake_blocks ===
lake_region_df = lake_blocks[['GridID', 'LakeRegion']].copy()

# === Merge onto fishnet; unmatched LakeRegion values are automatically NaN ===
fishnet = fishnet.merge(lake_region_df, on='GridID', how='left')

# === Replace NaN with -1 ===
fishnet['LakeRegion'] = fishnet['LakeRegion'].fillna(-1).astype(int)

# === Get grid dimensions ===
nrows = fishnet['row'].max() + 1
ncols = fishnet['col'].max() + 1

# === Initialize the LakeRegion grid matrix ===
lake_region_matrix = np.full((nrows, ncols), -1, dtype=int)

for _, row in fishnet.iterrows():
    i, j = row['row'], row['col']
    lake_region_matrix[i, j] = row['LakeRegion']

# === Initialize the output dictionary ===
region_to_edgegrid = defaultdict(list)

# === Identify edge cells for each LakeRegion ===
for _, row in fishnet.iterrows():
    i, j = row['row'], row['col']
    region = row['LakeRegion']
    
    if region == -1:
        continue  # Skip non-lake cells.
    
    neighbors = lake_region_matrix[max(i-1, 0):min(i+2, nrows),
                                   max(j-1, 0):min(j+2, ncols)]
    
    # If neighbors contain a different region, including -1, then the cell is an edge cell.
    # if np.any((neighbors != region) & (neighbors != -1)):
    if np.any(neighbors ==  -1):
        region_to_edgegrid[region].append(row['GridID'])



from collections import defaultdict

# === Initialize field ===
fishnet['IsEdge'] = 0

# === Mark edge cells ===
# First construct a set for fast lookup.
edge_gridid_set = set()
for gridids in region_to_edgegrid.values():
    edge_gridid_set.update(gridids)

# Write to the field.
fishnet['IsEdge'] = fishnet['GridID'].apply(lambda x: 1 if x in edge_gridid_set else 0)
fishnet.to_file('./data/Fishnet_Lake_Region.shp')

# Each LakeRegion has several edge cells.
# Find pure land cells within the third-order neighborhood of edge cells.
from collections import defaultdict
import geopandas as gpd
from tqdm import tqdm

fishnet = gpd.read_file('./data/Fishnet_Lake_Region.shp')

# === Initialize the output structure ===
region_edge_to_lands = defaultdict(dict)

# === Construct auxiliary mappings: position → GridID, Type ===
pos_to_gridid = {}
pos_to_type = {}
for _, row in fishnet.iterrows():
    pos = (row['row'], row['col'])
    pos_to_gridid[pos] = row['GridID']
    pos_to_type[pos] = row['Type']

# === Iterate over edge cells in each LakeRegion ===
for region in tqdm(region_to_edgegrid, desc="🔍 LakeRegion", unit="region"):
    edge_gridids = region_to_edgegrid[region]
    for edge_id in edge_gridids:
        row_data = fishnet.loc[fishnet['GridID'] == edge_id]
        if row_data.empty:
            continue
        i, j = int(row_data['row'].values[0]), int(row_data['col'].values[0])
        
        land_neighbors = set()
        # Third-order neighborhood, 7x7.
        for di in range(-3, 4):
            for dj in range(-3, 4):
                ni, nj = i + di, j + dj
                if 0 <= ni < nrows and 0 <= nj < ncols:
                    pos = (ni, nj)
                    if pos in pos_to_type and pos_to_type[pos] == 'land':
                        land_neighbors.add(pos_to_gridid[pos])
        
        # Store into the structure.
        region_edge_to_lands[region][edge_id] = land_neighbors


region_to_lands = defaultdict(set)

# === Iterate over the original result structure and extract all land cells ===
for region, edge_dict in region_edge_to_lands.items():
    for land_set in edge_dict.values():
        region_to_lands[region].update(land_set)

import json

# Convert sets to lists for serialization.
region_to_lands_serializable = {
    int(region): list(lands) for region, lands in region_to_lands.items()
}

# Save as a JSON file.
with open('./result/RegionToLandGridIDs.json', 'w') as f:
    json.dump(region_to_lands_serializable, f)

#%%
# Part1.2 Process temperature data for subsequent calculations.
import os
import numpy as np
import pandas as pd
import xarray as xr
import json
from tqdm import tqdm

# === Path configuration ===
t2m_path = r"D:\FPV\ERA5Land\Monthly_t2m.nc"
gridid_map_path = r"./data/GridID.npy"
region_land_json = r"./result/RegionToLandGridIDs.json"
output_path = r"./result/LandGrid_t2m.parquet"

# === Load GridID mapping ===
GridID_Map = np.load(gridid_map_path)
rows, cols = np.where(GridID_Map != -1)
gid_to_index = {int(GridID_Map[i, j]): (i, j) for i, j in zip(rows, cols)}

# === Load Region → GridID mapping ===
with open(region_land_json, 'r') as f:
    region_to_land = json.load(f)

# === Extract all unique GridIDs ===
all_gids = set()
for gridid_list in region_to_land.values():
    all_gids.update(map(int, gridid_list))

# === Load t2m data ===
ds = xr.open_dataset(t2m_path)
t2m = ds['t2m'].values  # shape: (time, lat, lon)
time = pd.to_datetime(ds['valid_time'].values)
lat = ds['latitude'].values
nlat = t2m.shape[1]

# === Construct results ===
records = []
for gid in tqdm(sorted(all_gids), desc="提取 t2m"):
    if gid not in gid_to_index:
        continue
    i, j = gid_to_index[gid]
    i_flipped = nlat - 1 - i
    t2m_series = t2m[:, i_flipped, j] - 273.15  # Convert to Celsius.
    for k, date in enumerate(time):
        records.append((gid, date.strftime("%Y-%m"), t2m_series[k]))

# === Save results ===
df = pd.DataFrame(records, columns=['GridID', 'date', 't2m'])
df.to_parquet(output_path, index=False)

# === Path configuration ===
t2m_parquet_path = "./result/LandGrid_t2m.parquet"
region_json_path = "./result/RegionToLandGridIDs.json"
output_path = "./result/RegionMeanT2M.parquet"

# === Load data ===
df = pd.read_parquet(t2m_parquet_path)  # Contains GridID, date, and t2m.
with open(region_json_path, 'r') as f:
    region_to_grids = json.load(f)
region_to_grids = {int(k): [int(x) for x in v] for k, v in region_to_grids.items()}

# === Get all dates for iteration ===
all_dates = df['date'].unique()
df.set_index(['GridID', 'date'], inplace=True)

# === Iterate over each region and date to calculate the arithmetic mean ===
records = []
for region, grid_list in tqdm(region_to_grids.items(), desc="区域均值计算"):
    for date in all_dates:
        values = []
        for gid in grid_list:
            try:
                t2m = df.loc[(gid, date), 't2m']
                if not pd.isna(t2m):
                    values.append(t2m)
            except KeyError:
                continue  # A given cell has no data for this date.
        if values:
            avg_t2m = sum(values) / len(values)
            records.append((region, date, avg_t2m))

# === Save results ===
df_result = pd.DataFrame(records, columns=['LakeRegion', 'date', 't2m'])
df_result.to_parquet(output_path, index=False)


#%%
#Part2 Calculate daily unit-area power generation for GPV and FPV.

import os
import pandas as pd
import numpy as np
from tqdm import tqdm
import json
from concurrent.futures import ProcessPoolExecutor

with open('./result/GridID_to_LakeRegion.json', 'r') as f:
    gid_to_region = json.load(f)
    gid_to_region = {int(k): int(v) for k, v in gid_to_region.items()}

Landt2m = pd.read_parquet("./result/RegionMeanT2M.parquet")
Landt2m['month_key'] = Landt2m['date'].astype(str).str[-2:]


# === Path configuration ===
input_dir = "D:/FPV/LakeArea/ssrd_zenith"
output_dir = "D:/FPV/LakeArea/pwr_new"
os.makedirs(output_dir, exist_ok=True)

# === Load static data ===
LakeFragments = pd.read_csv('./data/LakeFragments.csv')[['Hylak_id', 'GridID', 'FragID', 'AreaM2']]
PV_temp_df = pd.read_csv('./data/PV_temp.csv')[['Hylak_id', 'month_key', 'pv_temp']]
PV_temp_df['month_key'] = PV_temp_df['month_key'].astype(str).str.zfill(2)

# === PV estimation parameters ===
R_TMOD = 25
R_IRRADIANCE = 1000
SYSTEM_LOSS = 0.2
panel_ref_efficiency = 0.22
NOCT = 45 # Celsius.

def process_one_file(parquet_path):
    try:
        date_str = os.path.basename(parquet_path).replace(".parquet", "")
        df = pd.read_parquet(parquet_path)
        df.rename(columns={'FID': 'GridID'}, inplace=True)
        df['month_key'] = df['date'].str.split('_').str[1]
        
        df['LakeRegion'] = df['GridID'].map(gid_to_region)
        

        # Merge data.
        temp = pd.merge(LakeFragments, df[['GridID','LakeRegion', 'ssrd_sum', 'Theta', 'zenith', 'month_key']], on='GridID', how='left')
        temp = pd.merge(temp, PV_temp_df, on=['Hylak_id', 'month_key'], how='left')
        temp = pd.merge(temp,Landt2m[['LakeRegion','month_key','t2m']],on=['LakeRegion','month_key'],how='left')
        
        # Initialize output columns.
        temp['pv_mwh_per_m2'] = pd.NA
        temp['pv_mwh_per_m2_L'] = pd.NA
        temp['ssrd_corrected'] = pd.NA

        # Valid mask.
        valid_mask = (
            (~temp['zenith'].isna()) &
            (~temp['ssrd_sum'].isna()) &
            (~temp['pv_temp'].isna()) &
            (~temp['Theta'].isna()) &
            (temp['zenith'] < 90)
        )
        
        valid = temp.loc[valid_mask]

        # Vectorized calculation.
        T_PVST = valid['pv_temp'].values - 273.15 #modelled fpv temperature(see more in method)
        ssrd = valid['ssrd_sum'].values / (3600 * 24) #J/m2/day  -> W/m2
        Theta = np.radians(valid['Theta'].values)
        Zenith = np.radians(valid['zenith'].values)

        ssrd_corrected = np.maximum(ssrd * np.cos(Theta - Zenith), 0)
        
        # Calculate panel_relative_efficiency eff.

        G_ = ssrd_corrected / R_IRRADIANCE # Normalized irradiance; both variables have units of W/m2.
        T_ = T_PVST - R_TMOD # Subtract the standardized module temperature.

        log_G_ = np.full_like(G_, np.nan)
        positive_mask = G_ > 0
        log_G_[positive_mask] = np.log(G_[positive_mask])

        eff = np.zeros_like(G_)
        eff[positive_mask] = (
            1 +
            (-0.017162) * log_G_[positive_mask] +
            (-0.040289) * log_G_[positive_mask]**2 +
            T_[positive_mask] * (-0.004681 + 0.000148 * log_G_[positive_mask] + 0.000169 * log_G_[positive_mask]**2) +
            0.000005 * T_[positive_mask]**2
        )
        eff = np.maximum(eff, 0)
        
        # Calculate hourly power generation per unit area.
        # Since this is per unit area, panel_aperture == 1.
        panel_aperture = 1
        
        # Hourly power generation per unit area, in W.
        pv_power = (ssrd_corrected * panel_aperture 
                         * eff * panel_ref_efficiency)
        
        # Account for system loss.
        pv_power = pv_power * (1 - SYSTEM_LOSS) #W
        
  
        # Calculate daily power generation, in MWh.
        pv_mwh_per_m2 = (pv_power * 24) / 1_000_000  
        
        
        # Write results.
        temp.loc[valid_mask, 'ssrd_corrected'] = ssrd_corrected # W/m2
        temp.loc[valid_mask, 'pv_mwh_per_m2'] = pv_mwh_per_m2
        
        
        # Assume the PV panel is located on land.
        # Calculate operating temperature based on NOCT.
        # The unit of temp['ssrd_corrected'] is W/m2.
        temp['pv_temp_L'] = temp['t2m'] + temp['ssrd_corrected']/800 * (NOCT - 20)
        
        land_valid_mask = (~temp['pv_temp_L'].isna()) & (~temp['ssrd_corrected'].isna())

        # Vectorized calculation under the land-based assumption.
        T_PVST_L = temp.loc[land_valid_mask, 'pv_temp_L'].values.astype(np.float64)
        ssrd_L = temp.loc[land_valid_mask, 'ssrd_corrected'].values.astype(np.float64)

        G_L = ssrd_L / R_IRRADIANCE
        T_L = T_PVST_L - R_TMOD

        G_L = np.asarray(G_L, dtype=np.float64)
        log_G_L = np.where(G_L > 0, np.log(G_L), np.nan)

        # Calculate relative efficiency.
        eff_L = np.zeros_like(G_L)
        valid_idx = G_L > 0

        eff_L[valid_idx] = (
            1
            + (-0.017162) * log_G_L[valid_idx]
            + (-0.040289) * log_G_L[valid_idx] ** 2
            + T_L[valid_idx] * (
                -0.004681
                + 0.000148 * log_G_L[valid_idx]
                + 0.000169 * log_G_L[valid_idx] ** 2
            )
            + 0.000005 * T_L[valid_idx] ** 2
        )
        eff_L = np.maximum(eff_L, 0)
        
        # Calculate hourly power generation per unit area.
        # Since this is per unit area, panel_aperture == 1.
        panel_aperture = 1
        
        ssrd_L = temp.loc[land_valid_mask, 'ssrd_corrected'].values.astype(np.float64)

        
        # Hourly power generation per unit area, in W.
        pv_power_L = (ssrd_L * panel_aperture 
                         * eff_L * panel_ref_efficiency)

        
        # Account for system loss.
        pv_power_L = pv_power_L * (1 - SYSTEM_LOSS) #W
        
  
        # Calculate daily power generation, in MWh.
        pv_mwh_per_m2_L = (pv_power_L * 24) / 1_000_000  


        # Write results.
        temp.loc[land_valid_mask, 'pv_mwh_per_m2_L'] = pv_mwh_per_m2_L

        save_path = os.path.join(output_dir, f"{date_str}.csv")
        temp[['Hylak_id', 'FragID', 'pv_mwh_per_m2','pv_mwh_per_m2_L', 'ssrd_corrected']].to_csv(save_path, index=False)

        return f"{date_str} done"
    except Exception as e:
        return f"{parquet_path} error: {e}"

# === Process all files in parallel ===
if __name__ == "__main__":
    all_parquets = [os.path.join(input_dir, f) for f in os.listdir(input_dir) if f.endswith('.parquet')]
    with ProcessPoolExecutor(max_workers=18) as executor:
        results = list(tqdm(executor.map(process_one_file, all_parquets), total=len(all_parquets), desc="processing"))
        for res in results:
            print(res)






# %%
# Part3 Aggregate to monthly and country scales.



import pandas as pd
import geopandas as gpd
import os

Frags = gpd.read_file('./data/Lake_Fragments_Country_Updated.shp')

# Set directory path.
folder_path = r"D:\FPV\LakeArea\pwr_new"

# Get all daily files.
file_list = sorted([f for f in os.listdir(folder_path) if f.endswith('.csv')])

# Initialize the cumulative DataFrame.
cumulative_df = None

# Iterate over files.
for file_name in file_list:
    
    if file_name == '2023_12_31.csv':
        continue
    
    file_path = os.path.join(folder_path, file_name)
    df = pd.read_csv(file_path, usecols=["FragID", "Hylak_id", "pv_mwh_per_m2","pv_mwh_per_m2_L"])
    
    # Replace missing values with 0 to avoid missing-value propagation.
    df["pv_mwh_per_m2"] = df["pv_mwh_per_m2"].fillna(0)
    df["pv_mwh_per_m2_L"] = df["pv_mwh_per_m2_L"].fillna(0)
    
    if cumulative_df is None:
        cumulative_df = df.copy()
    else:
        cumulative_df["pv_mwh_per_m2"] += df["pv_mwh_per_m2"]
        cumulative_df["pv_mwh_per_m2_L"] += df["pv_mwh_per_m2_L"]
        # cumulative_df['pv_mwh_per_m2_simplified'_] += df['pv_mwh_per_m2_simplified']
        

# The final result is cumulative_df.
print(cumulative_df.head()) 
# cumulative_df.to_csv('./')

# Get Theta.
TimeZoneZenith = pd.read_csv('./data/TimeZone&Theta.csv')

df = pd.read_parquet(r"D:\FPV\LakeArea\ssrd\2024_01\2024_01_01.parquet")
df.rename(columns={'FID': 'GridID'}, inplace=True)

df['lon'] = df['lon'].apply(lambda x: x - 360 if x > 180 else x)

df['lat'] = df['lat'].round(4)
df['lon'] = df['lon'].round(4)
TimeZoneZenith['lat'] = TimeZoneZenith['lat'].round(4)
TimeZoneZenith['lon'] = TimeZoneZenith['lon'].round(4)

df = df.merge(TimeZoneZenith, on=['lat', 'lon'], how='left')

df = df[['GridID','Theta']]

Frags = Frags.merge(df,on=['GridID'], how='left')   

Frags = Frags.merge(cumulative_df,on=['Hylak_id','FragID'], how='left')

# Set FPV coverage constraints (see more in paper method).
import numpy as np
FPV_LIMIT_M2 = 30_000_000 #m2  # Maximum FPV area for a single lake.
FPV_RATIO = 0.1            # FPV coverage ratio.

results = []


grouped = Frags.groupby(['ISO_A3_EH', 'Hylak_id'])

for (country, lake_id), group_all in grouped:
    # Total area of all fragments, regardless of SA_type.
    total_area_all = group_all['AreaM2'].sum()

    # Deployable area.
    group_sa = group_all[group_all['SA_type'] == True].copy()
    total_area_sa = group_sa['AreaM2'].sum()

    # Initialize.
    total_power = 0.0
    total_power_L = 0.0
    used_area = 0.0

    # Calculate cos(theta).
    # Convert degrees to radians.
    group_sa['cos_theta'] = np.cos(np.radians(group_sa['Theta']))

    if total_area_sa == 0:
        total_power = 0.0
        total_power_L = 0.0
        used_area = 0.0
        continue    
    
    CurrentLimit = min(total_area_sa * FPV_RATIO, FPV_LIMIT_M2)
    
    group_sorted = group_sa.sort_values(by='pv_mwh_per_m2', ascending=False)
    accumulated_area = 0.0
    total_power = 0.0
    total_power_L = 0.0

    for _, row in group_sorted.iterrows():
        area = row['AreaM2']
        eff = row['pv_mwh_per_m2']
        eff_L = row['pv_mwh_per_m2_L']
        
        theta = np.cos(np.radians(row['Theta']))
        
        selected_area = min(area, CurrentLimit - accumulated_area)
        if selected_area <= 0:
            break
        
        total_power = eff * selected_area / theta
        total_power_L = eff_L * selected_area / theta
        
        results.append({
            'ISO_A3_EH': country,
            'Hylak_id': lake_id,
            'CFragID': row['CFragID'],
            'Max_FPV_MWh': total_power,
            'Max_FPV_MWh_L': total_power_L,
            'SelectedArea_m2': selected_area,
            'TotalArea_m2': row['AreaM2']
        })
            
        accumulated_area += selected_area

        if accumulated_area >= FPV_LIMIT_M2:
            break

    # Save results.

    


# Convert to DataFrame.
result_df = pd.DataFrame(results)

result_df.to_csv('./result/result_NOCT_new.csv',index=False)











# %%
# Calculate monthly power generation.
import os
import numpy as np
import pandas as pd
import geopandas as gpd
from collections import defaultdict
from tqdm import tqdm

# ========= Read fragment data and angle =========
Frags = gpd.read_file('./data/Lake_Fragments_Country_Updated.shp')
# Frags['SA_type'] = Frags['SA_type'].map({'T': True, 'F': False})

df_theta = pd.read_parquet(r"D:\FPV\LakeArea\ssrd\2024_01\2024_01_01.parquet")
df_theta.rename(columns={'FID': 'GridID'}, inplace=True)
df_theta['lon'] = df_theta['lon'].apply(lambda x: x - 360 if x > 180 else x)
df_theta['lat'] = df_theta['lat'].round(4)
df_theta['lon'] = df_theta['lon'].round(4)

TimeZoneZenith = pd.read_csv('./data/TimeZone&Theta.csv')
TimeZoneZenith['lat'] = TimeZoneZenith['lat'].round(4)
TimeZoneZenith['lon'] = TimeZoneZenith['lon'].round(4)

df_theta = df_theta.merge(TimeZoneZenith, on=['lat', 'lon'], how='left')[['GridID', 'Theta']]
Frags = Frags.merge(df_theta, on='GridID', how='left')

# ========= Read and accumulate all daily files =========
folder_path = r"D:\FPV\LakeArea\pwr_new"
file_list = sorted([f for f in os.listdir(folder_path) if f.endswith('.csv')])

cumulative_df = None
monthly_accumulated = defaultdict(lambda: None)

for file_name in tqdm(file_list, desc="loading"):
    
    if file_name == '2023_12_31.csv':
        continue
    
    file_path = os.path.join(folder_path, file_name)
    df = pd.read_csv(file_path, usecols=["FragID", "Hylak_id", "pv_mwh_per_m2", "pv_mwh_per_m2_L"])
    year, month = map(int, file_name.replace('.csv', '').split('_')[:2])
    key = (year, month)
    
    # Replace missing values with 0 to avoid missing-value propagation.
    df["pv_mwh_per_m2"] = df["pv_mwh_per_m2"].fillna(0)
    df["pv_mwh_per_m2_L"] = df["pv_mwh_per_m2_L"].fillna(0)


    if cumulative_df is None:
        cumulative_df = df.copy()
    else:
        cumulative_df["pv_mwh_per_m2"] += df["pv_mwh_per_m2"]
        cumulative_df["pv_mwh_per_m2_L"] += df["pv_mwh_per_m2_L"]

    if monthly_accumulated[key] is None:
        monthly_accumulated[key] = df.copy()
    else:
        monthly_accumulated[key]["pv_mwh_per_m2"] += df["pv_mwh_per_m2"]
        monthly_accumulated[key]["pv_mwh_per_m2_L"] += df["pv_mwh_per_m2_L"]

# ========= Merge annual total power-generation efficiency to prepare the deployment strategy =========
Frags = Frags.merge(cumulative_df, on=['FragID', 'Hylak_id'], how='left')

FPV_LIMIT_M2 = 30_000_000
FPV_RATIO = 0.1
deploy_records = []

grouped = Frags.groupby(['ISO_A3_EH', 'Hylak_id'])

for (country, lake_id), group_all in tqdm(grouped, desc="部署策略计算"):
    group_sa = group_all[group_all['SA_type'] == True].copy()
    total_area_sa = group_sa['AreaM2'].sum()
    if total_area_sa == 0:
        continue
    group_sa['cos_theta'] = np.cos(np.radians(group_sa['Theta']))

    CurrentLimit = min(total_area_sa * FPV_RATIO, FPV_LIMIT_M2)


    group_sorted = group_sa.sort_values(by='pv_mwh_per_m2', ascending=False)
    accumulated = 0.0
    for _, row in group_sorted.iterrows():
        area = row['AreaM2']
        selected_area = min(area, CurrentLimit - accumulated)
        if selected_area <= 0:
            break
        deploy_records.append({
            'FragID': row['FragID'],
            'CFragID': row['CFragID'],
            'Hylak_id': lake_id,
            'ISO_A3_EH': country,
            'SelectedArea_m2': selected_area
        })
        accumulated += selected_area
        if accumulated >= CurrentLimit:
            break

deployment_df = pd.DataFrame(deploy_records)


# ========= Monthly power generation at the country level =========
monthly_country_records = []


for (year, month), df_month in tqdm(monthly_accumulated.items(), desc="monthly processing"):
    
    # df_month is the accumulated unit-area power generation for the current month.
    
    df_merge = deployment_df.merge(df_month, on=['FragID', 'Hylak_id'], how='left')


    
    # Add angle information.
    df_merge = df_merge.merge(Frags[['CFragID', 'Theta']], on='CFragID', how='left')

    # Add cos(theta).
    df_merge['cos_theta'] = np.cos(np.radians(df_merge['Theta']))
    
    # Use angle-corrected power generation. The PV panel area also needs to be corrected because the panels are tilted rather than horizontal.
    df_merge['FPV_MWh']   = df_merge['pv_mwh_per_m2']   * df_merge['SelectedArea_m2'] / df_merge['cos_theta']
    df_merge['FPV_MWh_L'] = df_merge['pv_mwh_per_m2_L'] * df_merge['SelectedArea_m2'] / df_merge['cos_theta']

    df_merge['Year'] = year
    df_merge['Month'] = month

    df_merge_grouped = df_merge.groupby(['Year', 'Month', 'ISO_A3_EH'])[['FPV_MWh', 'FPV_MWh_L']].sum().reset_index()
    monthly_country_records.append(df_merge_grouped)

monthly_by_country = pd.concat(monthly_country_records)
monthly_by_country.to_csv('./result/monthly_power_by_country.csv', index=False)
