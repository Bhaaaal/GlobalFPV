# Intersect LakeFrags with global country vector boundaries.
# This determines which country each Frag belongs to.
#%%
import geopandas as gpd

GlobalCountries = gpd.read_file('./data/ne_10m_admin_0_countries/ne_10m_admin_0_countries.shp')
# ISO_A3_EH SOVEREIGNT NAME ADMIN
GlobalCountries = GlobalCountries[['ISO_A3_EH', 'SOVEREIGNT', 'NAME', 'ADMIN','geometry']]
LakeFragsShp = gpd.read_file('./data/Lake_Fragments.shp')
LakeFragsShp = LakeFragsShp[['GridID','Hylak_id','FragID','Lake_name', 'geometry']]
# Ensure the coordinate reference systems are consistent, if you have not done this already.
assert GlobalCountries.crs == LakeFragsShp.crs, "crs error"

# Perform spatial overlay using intersection.

# Check whether each fragment is fully contained within a country boundary.
contained = gpd.sjoin(LakeFragsShp, GlobalCountries, predicate='within', how='inner')

# Then process the remaining fragments.
remaining_ids = set(LakeFragsShp['FragID']) - set(contained['FragID'])

LakeFrags_remaining = LakeFragsShp[LakeFragsShp['FragID'].isin(remaining_ids)]

# Use sjoin to select intersecting country-fragment combinations.
intersecting = gpd.sjoin(LakeFrags_remaining, GlobalCountries, predicate='intersects', how='inner')

# Perform overlay for these truly transboundary fragments.
overlay_result = gpd.overlay(intersecting, GlobalCountries, how='intersection')

overlay_result = overlay_result.loc[:, ~overlay_result.columns.str.endswith('_2')]

# 2. Rename all fields ending with _1 to their suffix-free versions.
overlay_result.columns = [
    col[:-2] if col.endswith('_1') else col
    for col in overlay_result.columns
]

contained2 = contained[['GridID','Hylak_id','FragID','Lake_name','ISO_A3_EH', 'SOVEREIGNT', 'NAME', 'ADMIN', 'geometry']]
overlay_result2 = overlay_result[['GridID','Hylak_id','FragID','Lake_name','ISO_A3_EH', 'SOVEREIGNT', 'NAME', 'ADMIN', 'geometry']]

# Merge the two results.

import pandas as pd
final_result = gpd.GeoDataFrame(
    pd.concat([contained2, overlay_result2], ignore_index=True),
    crs=LakeFragsShp.crs
)
final_result['CFragID'] = range(len(final_result))

final_result.to_file("./data/Lake_Fragments_Country.shp")