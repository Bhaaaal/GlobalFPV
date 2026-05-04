#%%
# Generate simplified economic-benefit dataset for bubble plots

import pandas as pd

# ========= 1. Read FPV annual generation =========
power = pd.read_csv(
    "./result/monthly_power_by_country.csv",
    encoding="utf-8-sig"
)

power = (
    power.groupby("ISO_A3_EH", as_index=False)["FPV_MWh"]
    .sum()
)

power['FPV_TWh'] = power['FPV_MWh'] / 1e6

power = power[['ISO_A3_EH', 'FPV_TWh']].rename(columns={
    'ISO_A3_EH': 'iso3'
})

power = power.dropna(subset=['FPV_TWh'])
power = power[power['FPV_TWh'] > 0].copy()


# ========= 2. Read electricity prices =========
price = pd.read_excel('./data/2024_ElectricityPrice.xlsx')

price = price[['ISO3', 'Rprice', 'Bprice']].rename(columns={
    'ISO3': 'iso3'
})


# ========= 3. Read LCOE =========
lcoe = pd.read_csv('./data/2024_LCOE.csv')

lcoe = lcoe[lcoe['Year'] == 2024].copy()
lcoe = lcoe.rename(columns={
    'Code': 'iso3',
    'Solar photovoltaic levelized cost of energy': 'LCOE'
})

lcoe = lcoe[['iso3', 'LCOE']]


# ========= 4. Read World Bank income class =========
wb_class = pd.read_csv('./data/2024_WB_Class.csv')
wb_class = wb_class.dropna(subset=['Class'])
wb_class = wb_class[['Code', 'Class']].rename(columns={
    'Code': 'iso3'
})


# ========= 5. Merge all required data =========
df = power.merge(price, on='iso3', how='left')
df = df.merge(lcoe, on='iso3', how='left')
df = df.merge(wb_class, on='iso3', how='left')


# ========= 6. Fill missing LCOE with global weighted-average LCOE =========
df['LCOE'] = df['LCOE'].fillna(0.04261977)


# ========= 7. Calculate unit and total net profit =========
df['FPV_NetProfit_R_perkWH'] = df['Rprice'] - df['LCOE']
df['FPV_NetProfit_B_perkWH'] = df['Bprice'] - df['LCOE']

df['FPV_NetProfit_R'] = df['FPV_NetProfit_R_perkWH'] * df['FPV_TWh'] * 1e9
df['FPV_NetProfit_B'] = df['FPV_NetProfit_B_perkWH'] * df['FPV_TWh'] * 1e9


# ========= 8. Keep only fields needed for plotting =========
out = df[[
    'iso3',
    'FPV_TWh',
    'FPV_NetProfit_R_perkWH',
    'FPV_NetProfit_B_perkWH',
    'FPV_NetProfit_R',
    'FPV_NetProfit_B',
    'Class'
]].copy()


# ========= 9. Save =========
out.to_csv('./result/FPVprofit.csv', index=False, encoding='utf-8-sig')
# %%
