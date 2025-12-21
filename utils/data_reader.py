# This module contains get functions for data stored in Parquet files
import pandas as pd

def _get_daily_fx_spot(start_dt: pd.Timestamp=None, end_dt: pd.Timestamp=None, ccys: list=None, UnitPerUSD: bool=True, pivot: bool=False) -> pd.DataFrame:
    """
    Retrieve daily FX spot rates from Parquet file for given date range and currencies.

    Args:
        start_dt: Start date as pandas Timestamp.
        end_dt: End date as pandas Timestamp.
        ccys: List of currency ISO codes to filter.
        UnitPerUSD: If True, return rates as units per USD; else return market based convention.
        pivot: If True, pivot the DataFrame to have currencies as columns.
    Returns:
        DataFrame with columns: ['asof_dt', 'currency', 'spot']
    """
    parquet_file = r"C:\Users\AK\Documents\Quant\quant-research-prototype\data\raw\fx\daily\exchange_rate.parquet"
    df = pd.read_parquet(parquet_file)

    if start_dt is None:
        start_dt = df['asof_dt'].min()
    if end_dt is None:
        end_dt = df['asof_dt'].max()
    if ccys is None:
        ccys = df['currency'].unique().tolist()


    # Filter by date range and currencies
    mask = (df['asof_dt'] >= start_dt) & (df['asof_dt'] <= end_dt) & (df['currency'].isin(ccys)) & (df['tenor_days'] == 0)
    column_name = 'UnitPerUSD' if UnitPerUSD else 'rate'
    filtered_df = df.loc[mask, ['asof_dt', 'currency', column_name]]

    # Rename columns for clarity
    filtered_df = filtered_df.rename(columns={column_name: 'spot'})

    if pivot:
        filtered_df = filtered_df.pivot(index='asof_dt', columns='currency', values='spot')

    return filtered_df

def _get_daily_fx_forward(start_dt: pd.Timestamp=None, end_dt: pd.Timestamp=None, ccys: list=None, tenor_days: int=None, UnitPerUSD: bool=True, pivot: bool=False) -> pd.DataFrame:
    """
    Retrieve daily FX forward rates from Parquet file for given date range, currencies, and tenor.

    Args:
        start_dt: Start date as pandas Timestamp.
        end_dt: End date as pandas Timestamp.
        ccys: List of currency ISO codes to filter.
        tenor_days: Tenor in days (e.g., 90 for 3M forward).
        UnitPerUSD: If True, return rates as units per USD; else return market based convention.
        pivot: If True, pivot the DataFrame to have currencies as columns.

    Returns:
        DataFrame with columns: ['asof_dt', 'currency', 'forward']
    """
    parquet_file = r"C:\Users\AK\Documents\Quant\quant-research-prototype\data\raw\fx\daily\exchange_rate.parquet"
    df = pd.read_parquet(parquet_file)

    if start_dt is None:
        start_dt = df['asof_dt'].min()
    if end_dt is None:
        end_dt = df['asof_dt'].max()
    if ccys is None:
        ccys = df['currency'].unique().tolist()
    if tenor_days is None:
        tenor_days = df['tenor_days'].unique().tolist()

    # Filter by date range, currencies, and tenor
    mask = (df['asof_dt'] >= start_dt) & (df['asof_dt'] <= end_dt) & (df['currency'].isin(ccys)) & (df['tenor_days'].isin(tenor_days))
    column_name = 'UnitPerUSD' if UnitPerUSD else 'rate'
    filtered_df = df.loc[mask, ['asof_dt', 'currency', column_name]]

    # Rename columns for clarity
    filtered_df = filtered_df.rename(columns={column_name: 'forward'})

    if pivot:
        filtered_df = filtered_df.pivot(index='asof_dt', columns='currency', values='forward')

    return filtered_df

def _get_daily_fx_carry(start_dt: pd.Timestamp=None, end_dt: pd.Timestamp=None, ccys: list=None, tenor_days: int=90, pivot: bool=False) -> pd.DataFrame:
    """
    Retrieve daily FX carry from Parquet file for given date range, currencies, and tenor.

    Args:
        start_dt: Start date as pandas Timestamp.
        end_dt: End date as pandas Timestamp.
        ccys: List of currency ISO codes to filter.
        tenor_days: Tenor in days (default is 90 for 3M carry).       
        pivot: If True, pivot the DataFrame to have currencies as columns.
    
    Returns:
        DataFrame with columns: ['asof_dt', 'currency', 'carry']
    """
    parquet_file = r"C:\Users\AK\Documents\Quant\quant-research-prototype\data\features\fx\daily\fx_carry.parquet"
    df = pd.read_parquet(parquet_file)

    if start_dt is None:
        start_dt = df['asof_dt'].min()
    if end_dt is None:
        end_dt = df['asof_dt'].max()
    if ccys is None:
        ccys = df['currency'].unique().tolist()

    # Filter by date range, currencies, and tenor
    mask = (df['asof_dt'] >= start_dt) & (df['asof_dt'] <= end_dt) & (df['currency'].isin(ccys)) & (df['tenor_days'] == tenor_days)
    filtered_df = df.loc[mask, ['asof_dt', 'currency', 'carry']]

    if pivot:
        filtered_df = filtered_df.pivot(index='asof_dt', columns='currency', values='carry')

    return filtered_df
