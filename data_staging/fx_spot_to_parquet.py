# This script loads data from a CSV and formats it into a Parquet file.
# This script will only be used once to get up and running.  A separate daily production process
# will use vendor API to load data going forward
import pandas as pd
import utils.data_utils as du

# Load data from CSV
spot_df = pd.read_csv(r"C:\Users\AK\Documents\Quant\quant-research-prototype\fx_spot.csv")
foward_points_df = pd.read_csv(r"C:\Users\AK\Documents\Quant\quant-research-prototype\fx_fwrd_points.csv")

# Set Date column to date format
spot_df.Date = spot_df.Date.apply(lambda x: pd.to_datetime(x))
foward_points_df.Date = foward_points_df.Date.apply(lambda x: pd.to_datetime(x))

# Unpivt the dataframes
spot_df = spot_df.melt(id_vars=["Date"], var_name="currency", value_name="spot")
foward_points_df = foward_points_df.melt(id_vars=["Date"], var_name="currency", value_name="fwd_points")

# merge together to caluclate the foward rate
fwd_df = pd.merge(spot_df, foward_points_df, on=["Date", "currency"])
fwd_df["fwd_rate"] = fwd_df["spot"] + fwd_df["fwd_points"]

# Prepare for parquet output
invert_rate = ['EUR','GBP','AUD','NZD']
output_table_columns = ['asof_dt','currency','tenor_days','tenor','rate','UnitPerUSD']


spot_df.columns=['asof_dt','currency','rate']
fwd_df=fwd_df[['Date','currency','fwd_rate']]
fwd_df.columns = ['asof_dt','currency','rate']

spot_df['tenor_days']=0
spot_df['tenor']='spot'
spot_df['UnitPerUSD'] = spot_df.apply(lambda x: 1.0/x['rate'] if x['currency'] in invert_rate else x['rate'],axis=1)

fwd_df.loc[:,'tenor_days']=90
fwd_df.loc[:,'tenor']='3M'
fwd_df.loc[:,'UnitPerUSD'] = fwd_df.apply(lambda x: 1.0/x['rate'] if x['currency'] in invert_rate else x['rate'],axis=1)

df = pd.concat([spot_df, fwd_df], axis=0)
# Save the DataFrame to a Parquet file
du.append_or_create_parquet(df[output_table_columns], 
                         r"C:\Users\AK\Documents\Quant\quant-research-prototype\data\raw\fx\daily\exchange_rate.parquet",
                         key_fields=['asof_dt','currency','tenor_days'])