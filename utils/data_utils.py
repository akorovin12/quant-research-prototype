## Data related utility functions
import pandas as pd
import os
from pathlib import Path
import duckdb

# Function that will append or create a Parquet file
# if data for a given key already exists, it will be replaced with the new data
def append_or_create_parquet(new_df: pd.DataFrame, parquet_file: str, key_fields: list):
    """
    Efficiently append or upsert rows into a Parquet file using DuckDB.

    Behavior:
    - If the Parquet file doesn't exist, write `new_df` to it.
    - If it exists, remove rows from the existing Parquet whose key (as defined
      by `key_fields`) appears in `new_df`, and then append `new_df`.

    This avoids loading the entire Parquet file into pandas by delegating the
    merge/IO work to DuckDB.

    Args:
        new_df: DataFrame containing new or updated rows.
        parquet_file: Path to Parquet file to write/merge into.
        key_fields: List of column names that form the key. Must be non-empty.

    Raises:
        ValueError: if key_fields is empty.
        KeyError: if any key field is missing from new_df.
    """
    # Basic validations
    if not key_fields:
        raise ValueError("key_fields must be a non-empty list of column names")
    missing = [k for k in key_fields if k not in new_df.columns]
    if missing:
        raise KeyError(f"The following key fields are missing from new_df: {missing}")

    # Deduplicate incoming data on key fields (keep last/latest)
    new_df = new_df.drop_duplicates(subset=key_fields, keep="last")

    path = Path(parquet_file)
    # Use posix path so DuckDB SQL path quoting works consistently on Windows
    parquet_posix = path.as_posix()

    # Use an in-memory DuckDB connection
    con = duckdb.connect(database=":memory:")
    try:
        # Register the incoming DataFrame as a temporary table
        con.register("new_data", new_df)

        if path.exists():
            # Build safe join condition quoting column names
            join_cond = " AND ".join([f'existing."{k}" = new_data."{k}"' for k in key_fields])

            # DuckDB will stream-read the Parquet file and write out a new Parquet
            # with the stale rows removed and the new rows appended.
            sql = f"""
            COPY (
                SELECT * FROM parquet_scan('{parquet_posix}') AS existing
                WHERE NOT EXISTS (SELECT 1 FROM new_data WHERE {join_cond})
                UNION ALL
                SELECT * FROM new_data
            ) TO '{parquet_posix}' (FORMAT PARQUET);
            """
            con.execute(sql)
        else:
            # File doesn't exist: write new_data directly to Parquet
            con.execute(f"COPY (SELECT * FROM new_data) TO '{parquet_posix}' (FORMAT PARQUET);")
    finally:
        con.close()