import polars as pl
import numpy as np
import data_loader.fx_loader as fxl
from utils.data_utils import append_or_create_parquet


def calc_fx_total_return(spot_t_minus_n, spot_t, carry_t_minus_n, carry_t, n_days=1, tenor='3M'):
    """
    Calculate the total return of an FX position over a specified period, considering both spot price changes and carry from forward points.
    Assume all exchange rates are expressed USDXXX
    i.e +1% MXN return means a USD investor would make 1% if they were short USDMXN.

    Parameters:
    spot_t_minus_n (float): The spot exchange rate at the beginning of the period (t-n).
    spot_t (float): The spot exchange rate at the end of the period (t).
    forward_t_minus_n (float): The forward exchange rate at the beginning of the period (t-n).
    forward_t (float): The forward exchange rate at the end of the period (t).
    n_days (int): The number of days in the period. Default is 1 day.
    tenor (str): The tenor of the forward contract, e.g., '1M', '3M', '6M'. Default is '3M'.

    Returns:
    list: [spot_return, carry_return, total_return]
        spot_return (float): The return from spot price movement over the period, as a decimal.
        carry_return (float): The return from the change in forward points (carry) over the period, annualized to the tenor and scaled to the period length.
        total_return (float): The sum of spot_return and carry_return, representing the total FX return over the period.
    """
    # First lets convert exchange rates units per USD
    S_t_minus_n = 1 / spot_t_minus_n
    S_t = 1 / spot_t

    # Calculate spot return
    spot_return = S_t / S_t_minus_n - 1

    # Calculate carry return
    tenor_days_map = {'1M': 21, '3M': 65, '6M': 130, '1Y': 260}  # Approximate trading days for common tenors
    tenor_days_val = tenor_days_map.get(tenor)  
    
    # Calculate the total return
    total_return = (S_t - S_t_minus_n*np.exp(carry_t / 4 *(tenor_days_val-n_days) / tenor_days_val) / np.exp(carry_t_minus_n / 4)) / S_t_minus_n

    # Back out the carry return
    carry_return = total_return - spot_return

    return spot_return, carry_return, total_return

# Forcing to only use 3M carry and 1-day total returns for now.
# Can make this more robust in the future but for now its not needed
def fx_return_to_parquet(start_dt=None, end_dt=None, ccys=None,tenor='3M', n_days=1):
    """
    Calculate FX total returns for the specified tenor and save the results to a Parquet file.

    input parameters:
        start_dt (datetime, optional): Start date for filtering data.
        end_dt (datetime, optional): End date for filtering data.   
        ccys (list, optional): List of ISO currency codes to filter.
        tenor (str): The tenor of the forward contract, e.g., '1M', '3M', '6M'. Default is '3M'.
        n_days (int): The number of days in the period. Default is 1 day.

    Returns:
        None: The function saves the calculated FX return data to a Parquet file.
    """
    output_parquet_file = r"C:\Users\AK\Documents\Quant\quant-research-prototype\data\processed\daily\asset_returns\fx_return.parquet"
    
    # Get spot data
    spot_df = fxl._get_daily_fx_spot(start_dt=start_dt, end_dt=end_dt, ccys=ccys)
    spot_df = spot_df.rename({"close": "spot"})

    # Get carry data (3M carry only for now)
    carry_df = fxl._get_daily_fx_carry(start_dt=start_dt, end_dt=end_dt, tenor_days=90, ccys=ccys)

    # Merge spot and carry data on date and iso
    merged_df = spot_df.join(carry_df, on=['date', 'iso','year'], how='inner')

    # Backfill missing values with previous days values to ensure continuity in the data
    merged_df = merged_df.sort(['iso', 'date']).with_columns(
                    [pl.col(col).fill_null(strategy='forward').over(
                        ['iso']) for col in merged_df.columns if col not in ['iso', 'date']])
    
    # If still missing values, drop those rows as they cannot be calculated for returns
    merged_df = merged_df.drop_nulls(subset=['spot', 'carry'])

    # Calculate lagged values for spot and carry
    merged_df = merged_df.with_columns([
        pl.col('spot').shift(n_days).alias('spot_t_minus_n'),
        pl.col('carry').shift(n_days).alias('carry_t_minus_n')
    ])

    # Drop rows with null values in lagged columns
    merged_df = merged_df.drop_nulls(subset=['spot_t_minus_n', 'carry_t_minus_n'])

    # Calculate returns
    merged_df = merged_df.with_columns(
        pl.struct(['spot_t_minus_n', 'spot', 'carry_t_minus_n', 'carry']).map_elements(
            lambda s: calc_fx_total_return(
                s['spot_t_minus_n'], s['spot'], s['carry_t_minus_n'], s['carry'], 
                n_days=n_days, tenor=tenor
            ),
            return_dtype=pl.List(pl.Float64)  # Since your function returns a list of floats
        ).alias("returns")
    ).with_columns([
        pl.col("returns").list.get(0).alias("spot_return"),
        pl.col("returns").list.get(1).alias("carry_return"),
        pl.col("returns").list.get(2).alias("total_return")
    ]).drop("returns")

    # Select only relevant columns for output
    merged_df = merged_df.select([
        pl.col('date'),       
        pl.col('iso'),
        pl.col('tenor_days'),
        pl.col('spot_return'),
        pl.col('carry_return'),
        pl.col('total_return'),
        pl.col('year')])
    
    # Save to Parquet
    append_or_create_parquet(merged_df, output_parquet_file, key_fields=['date', 'iso','year'])
    print (f"FX return data for inputs tenor={tenor}, n_days={n_days}, start_dt={start_dt}, end_dt={end_dt}, ccys={ccys} saved to Parquet successfully.")
    return
    