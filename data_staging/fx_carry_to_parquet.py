# Calculate FX carry and store the results in Parquet format
import pandas as pd
from utils import data_reader as dr, data_utils as du
from feature_engineering import fx_feature_calcs as fx_calc

# Function to calculate FX carry and save to Parquet for specified start, end dates.
# Can run for all currencies or a subset of currencies.
# Can run for all tenors or a specific tenor.
#TODO: deal with multiple tenors.  Right no we only have one so it works fine.
def fx_carry_to_parquet(tenor_days=[90], start_dt=None, end_dt=None, ccys=None):
    """
    Calculate FX carry for the specified tenor and save the results to a Parquet file.

    input parameters:
        tenor_days (list, optional): list of tenors to run carry calculation for. If None, will run for all tenors found in data.
        start_dt (datetime, optional): Start date for filtering data.
        end_dt (datetime, optional): End date for filtering data.
        ccys (list, optional): List of ISO currency codes to filter.

    Returns:
        None: The function saves the calculated carry data to a Parquet file.
    """
    output_parquet_file = r"C:\Users\AK\Documents\Quant\quant-research-prototype\data\features\fx\daily\fx_carry.parquet"

    forward_df = dr._get_daily_fx_forward(start_dt=start_dt, end_dt=end_dt, tenor_days=tenor_days, ccys=ccys, UnitPerUSD=True, pivot=False)
    spot_df = dr._get_daily_fx_spot(start_dt=start_dt, end_dt=end_dt, ccys=ccys, UnitPerUSD=True, pivot=False)
    
    merged_df = pd.merge(forward_df, spot_df, on=['asof_dt', 'currency'], how='left')
    merged_df['tenor_days'] = tenor_days[0]
    # TODO:  deal with missing values
    
    # Calculate carry
    merged_df['carry'] = merged_df.apply(
        lambda row: fx_calc.calc_fx_carry(
            row['spot'],
            row['forward'],
            row['tenor_days']
        ), axis=1
    )

    # Select only relevant columns for output
    output_df = merged_df[['asof_dt', 'currency', 'tenor_days', 'carry']]

    # Append or create parquet file with the calculated carry data
    du.append_or_create_parquet(output_df, output_parquet_file, key_fields=['asof_dt', 'currency', 'tenor_days'])
    print(f"FX carry data for inputs tenor_days={tenor_days}, start_dt={start_dt}, end_dt={end_dt}, ccys={ccys} saved to Parquet successfully.")
    return





