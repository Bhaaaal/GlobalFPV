#%%
# Beluco
import pandas as pd
import numpy as np

# ======================
# 0) 参数
# ======================
YEAR = 2024
DEMAND_CSV = r"C:\Users\HuZheng\Desktop\FPV2.0\result\GlobalDemand.csv"
SOLAR_CSV  = r"./result/monthly_power_by_country.csv"   # 需含: Year, Month, ISO_A3_EH, FPV_MWh
HYDRO_CSV  = r"./result/GlobalHydro.csv"                # 需含: ISO 3 code, Date(YYYY/MM/DD), Value[, Unit]
EXCLUDE_ISO = {'KGZ','LKA','MDA','SLV'}                 # 需要去掉的国家, nodata

# 单位换算至 GWh
UNIT_TO_GWH = {
    'GWH': 1.0,
    'TWH': 1000.0,
    'MWH': 1e-3,
    'KWH': 1e-6
}
def to_gwh(value, unit):
    u = (str(unit) if pd.notna(unit) else '').strip().upper()
    return float(value) * UNIT_TO_GWH[u]

# ======================
# 1) 读数据并统一到 GWh/月
# ======================
# Solar
SolarData = pd.read_csv(SOLAR_CSV)
assert {'Year','Month','ISO_A3_EH','FPV_MWh'}.issubset(SolarData.columns)
SolarData['SolarGWh'] = SolarData['FPV_MWh'] / 1000.0
SolarData = SolarData[['Year','Month','ISO_A3_EH','SolarGWh']]

# Hydro
HydroRaw = pd.read_csv(HYDRO_CSV)
HydroRaw['Year']  = pd.to_datetime(HydroRaw['Date']).dt.year
HydroRaw['Month'] = pd.to_datetime(HydroRaw['Date']).dt.month
HydroRaw['ISO_A3_EH'] = HydroRaw['ISO 3 code']
if 'Unit' in HydroRaw.columns:
    HydroRaw['HydroGWh'] = HydroRaw.apply(lambda r: to_gwh(r['Value'], r['Unit']), axis=1)
else:
    # 若源文件无 Unit，按 TWh 处理（如已是 GWh，请改为 1.0）
    HydroRaw['HydroGWh'] = HydroRaw['Value'] * 1000.0
HydroData = HydroRaw[['Year','Month','ISO_A3_EH','HydroGWh']]

# Demand（用作月需求 Emc 的基准）
DemRaw = pd.read_csv(DEMAND_CSV)
DemRaw['Year']  = pd.to_datetime(DemRaw['Date']).dt.year
DemRaw['Month'] = pd.to_datetime(DemRaw['Date']).dt.month
DemRaw['ISO_A3_EH'] = DemRaw['ISO 3 code']
Dem = DemRaw[DemRaw['Variable'].str.lower()=='demand'].copy()
if 'Unit' in Dem.columns:
    Dem['DemandGWh'] = Dem.apply(lambda r: to_gwh(r['Value'], r['Unit']), axis=1)
else:
    # 若无 Unit，按 TWh 处理（如已是 GWh，请改为 1.0）
    Dem['DemandGWh'] = Dem['Value'] * 1000.0
DemandMonthly = Dem[['Year','Month','ISO_A3_EH','DemandGWh']]

# ======================
# 2) 合并到目标年度并排除指定国家
# ======================
DF = (SolarData.merge(HydroData, on=['ISO_A3_EH','Year','Month'], how='inner')
                .merge(DemandMonthly, on=['ISO_A3_EH','Year','Month'], how='inner'))
DF = DF[(DF['Year']==YEAR) & (~DF['ISO_A3_EH'].isin(EXCLUDE_ISO))].copy()

# 要求每国恰好 12 个月
cnt = DF.groupby('ISO_A3_EH')['Month'].nunique()
lack = cnt[cnt!=12]
if not lack.empty:
    raise ValueError(f"{YEAR} 年份以下国家月份不足 12：{lack.index.tolist()}")

# 计算各国 Emc（GWh/月）：年度总需求/12（或 12 个月均值，等价）
AnnualDemand = DF.groupby('ISO_A3_EH', as_index=False)['DemandGWh'].sum().rename(columns={'DemandGWh':'AnnualDemand_GWh'})
AnnualDemand['Emc'] = AnnualDemand['AnnualDemand_GWh'] / 12.0
Emc_map = dict(zip(AnnualDemand['ISO_A3_EH'], AnnualDemand['Emc']))

# ======================
# 3) 纯月度改写版 Beluco 指数（k_e 采用原文绝对值形式）
# ======================
def beluco_monthly_pure(solar_m, hydro_m, Emc):
    s = np.asarray(solar_m, dtype=float)
    h = np.asarray(hydro_m, dtype=float)
    if s.size!=12 or h.size!=12 or Emc is None or not np.isfinite(Emc) or Emc<=0:
        return np.nan, np.nan, np.nan, np.nan

    # k_t：最小值所在月份的环绕距离 / 6
    ms = int(np.argmin(s))
    mh = int(np.argmin(h))
    dm = abs(ms - mh)
    k_t = min(dm, 12 - dm) / 6.0
    k_t = float(np.clip(k_t, 0.0, 1.0))

    # k_e：绝对值形式（与原文 Eq.(4) 一致）
    Es = float(s.sum()); Eh = float(h.sum())
    if (Es + Eh) == 0:
        k_e = 0.0
    else:
        k_e = 1.0 - abs(Eh - Es) / (Eh + Es)
    k_e = float(np.clip(k_e, 0.0, 1.0))

    # k_a：月度分段（以 Emc 归一化）
    ds = 1.0 + (float(s.max() - s.min())) / Emc
    dh = 1.0 + (float(h.max() - h.min())) / Emc
    denom_left = (1.0 - ds)**2
    if dh <= ds:
        k_a = 0.0 if denom_left==0.0 else 1.0 - ((dh - ds)**2) / denom_left
    else:
        denom_tot = denom_left + (dh - ds)**2
        k_a = 0.0 if denom_tot==0.0 else denom_left / denom_tot
    k_a = float(np.clip(k_a, 0.0, 1.0))

    k = k_t * k_e * k_a
    return k_t, k_e, k_a, k

def _apply_group(g):
    iso = g.name
    Emc = Emc_map.get(iso, np.nan)
    s = g.sort_values('Month')['SolarGWh'].to_numpy()
    h = g.sort_values('Month')['HydroGWh'].to_numpy()
    kt, ke, ka, k = beluco_monthly_pure(s, h, Emc)
    return pd.Series({'k_t':kt, 'k_e':ke, 'k_a':ka, 'k':k})

BelucoResults = DF.groupby('ISO_A3_EH').apply(_apply_group).reset_index()
BelucoResults.to_csv('./BelucoResults.csv')
