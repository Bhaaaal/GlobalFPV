# This script generates ERA5-Land-based global fishnet grids, converts the longitude range from 0–360 to -180–180, and overlays the grid with HydroLAKES and country boundaries. 
# It then produces grid-level masks and index matrices for identifying lake-covered cells, country-covered cells, and land-only cells, which can be used for subsequent spatial extraction and analysis of ERA5-Land variables.
#%% 
# Generate a FishNet grid based on ERA5-Land
# The coordinate system of ERA5-Land is EPSG:4326, with longitude ranging from 0 to 360.
# It should be converted to -180 to 180 when importing into ArcGIS.
# The Fishnet output with suffix 360 indicates a longitude range of 0 to 360.
# The Fishnet output with suffix 180 indicates a longitude range of -180 to 180.
# The row and col fields in the Fishnet are generated based on ERA5-Land under the 360-degree rule and will not be transformed later.
import xarray as xr
import geopandas as gpd
import pandas as pd
from shapely.geometry import box

# Open the NetCDF file, which is the original ERA5-Land nc file.
nc_path = r"G:\ERA5-2024\ice\ERA5Land_2024_01_SSRD_ICE.nc"
ds = xr.open_dataset(nc_path)

# Extract latitude and longitude.
# In fact, the latitude dimension of ERA5-Land is descending, e.g., [90., 89.9, 89.8, 89.7, 89.6].
# However, the Fishnet constructed here uses ascending latitude.
# Therefore, this should be handled carefully in subsequent extraction.
lats = sorted(ds['latitude'].values)
lons = sorted(ds['longitude'].values)

lat_res = round(lats[1] - lats[0], 6)
lon_res = round(lons[1] - lons[0], 6)

# Construct grid polygons based on cell centers plus/minus half a grid cell.
polygons = []
index = []

for i, lat in enumerate(lats):
    for j, lon in enumerate(lons):
        lat_min = lat - lat_res / 2
        lat_max = lat + lat_res / 2
        lon_min = lon - lon_res / 2
        lon_max = lon + lon_res / 2

        poly = box(lon_min, lat_min, lon_max, lat_max)
        polygons.append(poly)
        index.append((i, j))

# Construct the GeoDataFrame.
gdf = gpd.GeoDataFrame(index=pd.Index(range(len(polygons))), geometry=polygons, crs="EPSG:4326")
gdf["row"], gdf["col"] = zip(*index)
gdf["FID"] = gdf.index
gdf["lon"] = [lons[j] for i, j in index]
gdf["lat"] = [lats[i] for i, j in index]
gdf.to_file('./data/Fishnet_360.shp')


# Output the version with longitude ranging from -180 to 180.
gdf_180 = gdf.copy()
def shift_lon_180(poly):
    minx, miny, maxx, maxy = poly.bounds
    if minx >= 180:
        minx -= 360
        maxx -= 360
    return box(minx, miny, maxx, maxy)
# Apply the conversion.
gdf_180["geometry"] = gdf_180["geometry"].apply(shift_lon_180)
gdf_180["lon"] = gdf_180["lon"].apply(lambda x: x - 360 if x >= 180 else x)

gdf_180.to_file('./data/Fishnet_180.shp')



#%%
# Intersection overlay between Fishnet and HydroLakes.
# The GridID field indicates the Fishnet index starting from 0.
# HydroLakes uses a longitude range of -180 to 180, so the corresponding Fishnet_180 should be used.
# FragID is the lake fragment ID.
# The outputs Lake_Fragment.shp and Fishnet_180_HasLake.shp both use the -180 to 180 longitude range.
import geopandas as gpd

# 1. Read Fishnet and Lakes.
fishnet = gpd.read_file('./data/Fishnet_180.shp')
fishnet['GridID'] = fishnet.index  # Add a unique identifier.
lakes = gpd.read_file(r'./HydroLAKES_polys_v10_shp/HydroLAKES_polys_v10.shp')
lakes = lakes[['Hylak_id', 'Lake_name', 'Country', 'geometry']]  # Keep key fields.

# 2. Unify coordinate systems.
if lakes.crs != fishnet.crs:
    lakes = lakes.to_crs(fishnet.crs)

# 3. Overlay intersection: cut lake polygons into fragments by Fishnet.
lake_fragments = gpd.overlay(fishnet, lakes, how='intersection')

# 4. Add the HasLake field to the original Fishnet, marking which GridIDs are used.
haslake_ids = lake_fragments['GridID'].unique()
fishnet['HasLake'] = fishnet['GridID'].isin(haslake_ids).astype(int)

lake_fragments['FragID'] = lake_fragments.index
lake_fragments.to_file('./data/Lake_Fragments.shp')
fishnet.to_file('./data/Fishnet_180_HasLake.shp')


#%%
# Generate files for extracting data from HasLake regions based on './data/Fishnet_180_HasLake.shp'.

import numpy as np
import geopandas as gpd

fishnet = gpd.read_file('./data/Fishnet_180_HasLake.shp')
# Get grid dimensions.
nrows = fishnet['row'].max() + 1
ncols = fishnet['col'].max() + 1
print(f"检测网格维度：({nrows}, {ncols})")

# Initialize matrices.
HasLake = np.zeros((nrows, ncols), dtype=np.uint8)
GridID = np.full((nrows, ncols), fill_value=-1, dtype=np.int32)  # -1 indicates an invalid cell.

# Fill HasLake and GridID row by row.
for _, row in fishnet.iterrows():
    i = row['row']
    j = row['col']
    HasLake[i, j] = int(row['HasLake'])  # 0 or 1
    GridID[i, j] = int(row['GridID'])    # FID/Index

# Save as npy for subsequent use.
np.save('./data/HasLake.npy', HasLake)
np.save('./data/GridID.npy', GridID)

# %%
import xarray as xr
import numpy as np

# === Path settings ===
haslake_path = './data/HasLake.npy'
index_path = './data/Index.npy'
nc_path = 'G:/ERA5-2024/ice/ERA5Land_2024_01_SSRD_ICE.nc'

# === Read the HasLake and Index matrices ===
HasLake = np.load(haslake_path)
Index = np.load(index_path)

# === Open the ERA5-Land NetCDF file ===
ds = xr.open_dataset(nc_path)
ssrd = ds['ssrd']

# === Extract dimension sizes ===
nlat = ssrd.sizes['latitude']
nlon = ssrd.sizes['longitude']
nc_shape = (nlat, nlon)

# === Compare matrix shapes ===
haslake_shape = HasLake.shape
index_shape = Index.shape

print("NetCDF shape:", nc_shape)
print("HasLake shape:", haslake_shape)
print("Index shape:", index_shape)



#%%
import geopandas as gpd
import numpy as np

# === Read data and perform spatial join ===
fishnet = gpd.read_file('./data/Fishnet_180.shp')
fishnet['GridID'] = fishnet.index

countries = gpd.read_file('./data/ne_10m_admin_0_countries/ne_10m_admin_0_countries.shp')
countries = countries[['ISO_A3_EH', 'NAME', 'geometry']]

# Unify coordinate systems.
if fishnet.crs != countries.crs:
    countries = countries.to_crs(fishnet.crs)

# Spatial join: only retain grid cells that are completely within countries.
fishnet_within_country = gpd.sjoin(fishnet, countries, how='left', predicate='within')

# === Grid dimensions ===
nrows = fishnet_within_country['row'].max() + 1
ncols = fishnet_within_country['col'].max() + 1

# === Initialize matrices ===
HasCountry = np.zeros((nrows, ncols), dtype=np.uint8)
CountryCode = np.full((nrows, ncols), '', dtype='<U3')  # Country code with up to 3 characters.

# === Fill matrices ===
for _, row in fishnet_within_country.iterrows():
    i = row['row']
    j = row['col']
    HasCountry[i, j] = 1
    CountryCode[i, j] = row['ISO_A3_EH']

# === Save ===
np.save('./data/HasCountry.npy', HasCountry)
np.save('./data/CountryCode.npy', CountryCode)



#%%
import geopandas as gpd
import numpy as np
import xarray as xr

# === Read the Fishnet grid ===
fishnet = gpd.read_file('./data/Fishnet_180.shp')
fishnet['GridID'] = fishnet.index

# === Read lake_cover data, which has been aligned to the ERA5 grid ===
ds_lake = xr.open_dataset(r"F:/LakeArea/ERA5Land_LakeCover.nc")
lake_cover = ds_lake['cl'].values  # shape: (lat, lon), range [0–1]\
lake_cover = np.squeeze(lake_cover)
lat = ds_lake['latitude'].values
lon = ds_lake['longitude'].values

# === Determine grid dimensions ===
nrows = fishnet['row'].max() + 1
ncols = fishnet['col'].max() + 1

# === Initialize the mask matrix ===
HasLand = np.zeros((nrows, ncols), dtype=np.uint8)

# === Iterate through all Fishnet cells and determine whether each cell is land based on (row, col) location in lake_cover ===
for _, row in fishnet.iterrows():
    i = row['row']
    j = row['col']
    
    # ERA5-Land latitude is ordered from north to south, so the i index needs to be flipped.
    lat_idx = lake_cover.shape[0] - 1 - i
    lon_idx = j

    # Check boundary validity.
    if lat_idx < 0 or lat_idx >= lake_cover.shape[0]:
        continue
    if lon_idx < 0 or lon_idx >= lake_cover.shape[1]:
        continue
    
    if lake_cover[lat_idx, lon_idx] == 0:  # Only retain areas without lakes.
            HasLand[i, j] = 1

# === Save output ===
np.save('./data/HasLand.npy', HasLand)