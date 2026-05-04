#%%
import pandas as pd
# Combine all frequency metrics.

licd = pd.read_csv('./CalStudyArea/freq/licd_freq.csv')
licd['licd_type'] = licd['licd_freq'] < 0.5

tp = pd.read_csv('./CalStudyArea/freq/tp_freq.csv')
tp['tp_type'] = tp['tp_freq'] < 0.1

ws =  pd.read_csv('./CalStudyArea/freq/ws_freq.csv')
ws['ws_type'] = ws['ws_freq'] < 0.1

ssrd = pd.read_csv('./CalStudyArea/freq/ssrd_sum.csv')
ssrd['ssrd_type'] = ssrd['above_threshold']
ssrd.drop(columns=['above_threshold',''], inplace=True)
ssrd.rename(columns={'FID': 'GridID'}, inplace=True)


ids_licd = set(licd['GridID'])
ids_tp = set(tp['GridID'])
ids_ws = set(ws['GridID'])
ids_ssrd = set(ssrd['GridID'])

all_equal = (ids_licd == ids_tp == ids_ws == ids_ssrd)

print("Whether all GridIDs are exactly consistent:", all_equal)


# Merge all data, using an inner join to retain only records valid in all datasets.
df_merged = licd.merge(tp, on='GridID', how='inner') \
                .merge(ws, on='GridID', how='inner') \
                .merge(ssrd, on='GridID', how='inner')

# Add the final judgment column indicating whether all conditions are satisfied.
df_merged['SA_type'] = (
    df_merged['licd_type'] &
    df_merged['tp_type'] &
    df_merged['ws_type'] &
    df_merged['ssrd_type']
)

df_merged.to_csv('./data/SA_type.csv',index=False)
