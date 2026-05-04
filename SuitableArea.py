#%%
# Determine SuitableArea by integrating frequency metrics, FPV temperature, and distance to population centers.
import os
import pandas as pd
import geopandas as gpd
import numpy as np
from shapely.strtree import STRtree

PATH_FRAGS = './data/Lake_Fragments_Country.shp'
PATH_SA    = './data/SA_type.csv'
PATH_POP   = './data/PopCenter.shp'
PATH_UPD   = './data/Lake_Fragments_Country_Updated.shp'
PATH_PVT   = './data/PV_temp.csv'

PATH_LICD  = './CalStudyArea/freq/licd_freq.csv'
PATH_TP    = './CalStudyArea/freq/tp_freq.csv'
PATH_WS    = './CalStudyArea/freq/ws_freq.csv'
PATH_SSRD  = './CalStudyArea/freq/ssrd_sum.csv'

OUT_DIR    = './result'
OUT_SHP    = os.path.join(OUT_DIR, 'SuitableArea.shp')
os.makedirs(OUT_DIR, exist_ok=True)

def pick_col(gdf, candidates):
    cols = list(gdf.columns)
    low  = {c.lower(): c for c in cols}
    for cand in candidates:
        if cand in cols: return cand
        if cand.lower() in low: return low[cand.lower()]
    for c in cols:
        cl = c.lower()
        if any(k in cl for k in ['pvtemp','temp']) and any(k in cl for k in ['valid','ok']):
            return c
    return None

def ids_from_csv(path, id_col='GridID', alt='FID'):
    if not os.path.exists(path): return set()
    df = pd.read_csv(path)
    if id_col not in df.columns and alt in df.columns:
        df = df.rename(columns={alt: id_col})
    return set(df[id_col].unique())

def tri_state(ok_bool: bool, is_nodata: bool) -> str:
    if is_nodata: return 'NODATA'
    return 'TRUE' if bool(ok_bool) else 'FALSE'

def coerce_bool_nullable(x):
    if pd.isna(x): return pd.NA
    if isinstance(x, (bool, np.bool_)): return bool(x)
    if isinstance(x, (int, float, np.integer, np.floating)):
        if pd.isna(x): return pd.NA
        return bool(int(x))
    s = str(x).strip().lower()
    if s in {'true','t','1','yes','y'}:  return True
    if s in {'false','f','0','no','n'}:  return False
    return pd.NA

# ========== 主处理流程 ==========
def main():
    Frags = gpd.read_file(PATH_FRAGS)
    Frags = Frags[['GridID','Hylak_id','FragID','Lake_name','geometry']]
    Frags = Frags.to_crs(epsg=8857)
    Frags['area_m2']  = Frags.geometry.area
    Frags['area_km2'] = Frags['area_m2'] / 1e6

    SA = pd.read_csv(PATH_SA)
    need_bool = ['GridID','licd_type','tp_type','ws_type','ssrd_type','SA_type']
    num_cols = {'licd_freq':'licd_freq','tp_freq':'tp_freq','ws_freq':'ws_freq','ssrd_sum_kwh':'ssrd_kwh'}
    SA_sub = SA[need_bool + [c for c in num_cols if c in SA.columns]].copy().rename(columns=num_cols)
    Frags = Frags.merge(SA_sub, on='GridID', how='left')
    for c in ['licd_type','tp_type','ws_type','ssrd_type','SA_type']:
        Frags[c] = Frags[c].fillna(False).astype(bool)

    if os.path.exists(PATH_UPD):
        upd = gpd.read_file(PATH_UPD)
        col_temp = pick_col(upd, ['ok_temp','pvtemp_valid','pvtemp_val'])
        col_pop  = pick_col(upd, ['ok_pop','near_popcenter_10km','near10km'])

        if 'FragID' in upd.columns:
            if col_temp:
                temp_df = upd[['FragID', col_temp]].drop_duplicates(subset=['FragID'])
                Frags = Frags.merge(temp_df.rename(columns={col_temp:'ok_temp_raw'}), on='FragID', how='left')

            if col_pop:
                pop_df = upd[['FragID', col_pop]].drop_duplicates(subset=['FragID'])
                Frags = Frags.merge(pop_df.rename(columns={col_pop:'ok_pop_raw'}), on='FragID', how='left')


    if 'ok_temp_raw' not in Frags.columns or Frags['ok_temp_raw'].isna().all():
        if os.path.exists(PATH_PVT):
            PV = pd.read_csv(PATH_PVT)
            tempC = PV['pv_temp_C'] if 'pv_temp_C' in PV.columns else PV['pv_temp'] - 273.15
            ok = tempC.between(-40,85)
            ratio = ok.groupby(PV['Hylak_id']).mean().rename('ratio').reset_index()
            valid = ratio.assign(ok_temp_raw=lambda x: x['ratio']>=0.99)[['Hylak_id','ok_temp_raw']]
            Frags = Frags.merge(valid, on='Hylak_id', how='left')
        else:
            Frags['ok_temp_raw'] = pd.NA
    Frags['ok_temp_raw'] = Frags['ok_temp_raw'].apply(coerce_bool_nullable)

    if 'ok_pop_raw' not in Frags.columns or Frags['ok_pop_raw'].isna().all():
        if os.path.exists(PATH_POP):
            pop = gpd.read_file(PATH_POP).to_crs(Frags.crs)
            if 'grid_code' in pop.columns:
                pop = pop[pop['grid_code']==1]
            if len(pop)>0:
                tree = STRtree(list(pop.geometry))
                def nearest_km(pt): return pt.distance(pop.geometry.iloc[tree.nearest(pt)]) / 1000.0
                Frags['pt'] = Frags.geometry.representative_point()
                Frags['pop_km'] = Frags['pt'].apply(nearest_km).astype('float32')
                Frags['ok_pop_raw'] = Frags['pop_km']<=10
                Frags = Frags.drop(columns=['pt'])
            else:
                Frags['pop_km'] = np.nan
                Frags['ok_pop_raw'] = pd.NA
        else:
            Frags['pop_km'] = np.nan
            Frags['ok_pop_raw'] = pd.NA
    Frags['ok_pop_raw'] = Frags['ok_pop_raw'].apply(coerce_bool_nullable)

    licd_ids = ids_from_csv(PATH_LICD)
    tp_ids   = ids_from_csv(PATH_TP)
    ws_ids   = ids_from_csv(PATH_WS)
    ssrd_ids = ids_from_csv(PATH_SSRD)
    Frags['ice_nd']  = (~Frags['GridID'].isin(licd_ids)) if licd_ids else Frags['licd_freq'].isna()
    Frags['tp_nd']   = (~Frags['GridID'].isin(tp_ids))   if tp_ids   else Frags['tp_freq'].isna()
    Frags['ws_nd']   = (~Frags['GridID'].isin(ws_ids))   if ws_ids   else Frags['ws_freq'].isna()
    Frags['ssrd_nd'] = (~Frags['GridID'].isin(ssrd_ids)) if ssrd_ids else Frags.get('ssrd_kwh', pd.Series(index=Frags.index)).isna()
    Frags['temp_nd'] = Frags['ok_temp_raw'].isna()
    Frags['pop_nd']  = Frags['ok_pop_raw'].isna()

    for col, nd in [('licd_freq','ice_nd'), ('tp_freq','tp_nd'), ('ws_freq','ws_nd'), ('ssrd_kwh','ssrd_nd')]:
        if col in Frags.columns:
            Frags.loc[Frags[nd], col] = -999.0
            Frags[col] = Frags[col].astype('float32').fillna(-999.0)

    Frags['ok_ice']  = [tri_state(o, n) for o, n in zip(Frags['licd_type'], Frags['ice_nd'])]
    Frags['ok_tp']   = [tri_state(o, n) for o, n in zip(Frags['tp_type'], Frags['tp_nd'])]
    Frags['ok_ws']   = [tri_state(o, n) for o, n in zip(Frags['ws_type'], Frags['ws_nd'])]
    Frags['ok_ssrd'] = [tri_state(o, n) for o, n in zip(Frags['ssrd_type'], Frags['ssrd_nd'])]
    Frags['ok_temp'] = [tri_state(bool(o), n) if not pd.isna(Frags['ok_temp_raw'].iloc[i]) else 'NODATA'
                        for i, (o, n) in enumerate(zip(Frags['ok_temp_raw'], Frags['temp_nd']))]
    Frags['ok_pop']  = [tri_state(bool(o), n) if not pd.isna(Frags['ok_pop_raw'].iloc[i]) else 'NODATA'
                        for i, (o, n) in enumerate(zip(Frags['ok_pop_raw'], Frags['pop_nd']))]

    any_nd = (Frags[['ice_nd','tp_nd','ws_nd','ssrd_nd','temp_nd','pop_nd']].any(axis=1))
    all_true = (
        (Frags['ok_ice']=='TRUE')  &
        (Frags['ok_tp']=='TRUE')   &
        (Frags['ok_ws']=='TRUE')   &
        (Frags['ok_ssrd']=='TRUE') &
        (Frags['ok_temp']=='TRUE') &
        (Frags['ok_pop']=='TRUE')
    )
    Frags['ok_all'] = np.where(any_nd, 'NODATA', np.where(all_true, 'TRUE', 'FALSE'))

    def fail_code_row(r):
        items = []
        def add(code, ok_str, nd):
            if nd: items.append(f'{code}_ND')
            elif ok_str != 'TRUE': items.append(code)
        add('ICE',  r['ok_ice'],  r['ice_nd'])
        add('TP',   r['ok_tp'],   r['tp_nd'])
        add('WS',   r['ok_ws'],   r['ws_nd'])
        add('SSRD', r['ok_ssrd'], r['ssrd_nd'])
        add('TEMP', r['ok_temp'], r['temp_nd'])
        add('POP',  r['ok_pop'],  r['pop_nd'])
        return 'PASS' if r['ok_all']=='TRUE' else ','.join(items)

    Frags['fail_code'] = Frags.apply(fail_code_row, axis=1)

    from shapely import GeometryCollection
    Frags = Frags[~Frags.geometry.apply(lambda g: isinstance(g, GeometryCollection) and g.is_empty)]

    out_cols = ['GridID','Hylak_id','FragID','Lake_name','area_m2','area_km2','pop_km',
                'ok_ice','ok_tp','ok_ws','ok_ssrd','ok_temp','ok_pop','ok_all',
                'ice_nd','tp_nd','ws_nd','ssrd_nd','temp_nd','pop_nd',
                'licd_freq','tp_freq','ws_freq','ssrd_kwh','geometry']
    gdf_out = gpd.GeoDataFrame(Frags[[c for c in out_cols if c in Frags.columns]].copy(),
                               geometry='geometry', crs=Frags.crs)
    gdf_out.to_file(OUT_SHP, encoding='utf-8')

    total_km2   = gdf_out['area_km2'].sum()
    usable_km2  = gdf_out.loc[gdf_out['ok_all'] == 'TRUE',   'area_km2'].sum()
    notok_km2   = gdf_out.loc[gdf_out['ok_all'] == 'FALSE',  'area_km2'].sum()
    nodata_km2  = gdf_out.loc[gdf_out['ok_all'] == 'NODATA', 'area_km2'].sum()

    print(f'Total lake area:        {total_km2:,.2f} km²')
    print(f'Usable (ok_all=TRUE):   {usable_km2:,.2f} km²')
    print(f'Not OK  (ok_all=FALSE): {notok_km2:,.2f} km²')
    print(f'NODATA  (ok_all=NODATA):{nodata_km2:,.2f} km²')

main()
