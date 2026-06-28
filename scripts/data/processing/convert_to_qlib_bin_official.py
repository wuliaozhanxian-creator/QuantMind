import pandas as pd
import os
import shutil
from pathlib import Path
import subprocess
import sys

def convert_with_official_tool(parquet_path, qlib_dir, qlib_scripts_path):
    print(f"Reading {parquet_path}...")
    df = pd.read_parquet(parquet_path)
    
    # Qlib official tool expects 'date' and 'symbol'
    df = df.rename(columns={'trade_date': 'date'})
    
    # Create temporary CSV directory
    tmp_csv_dir = Path("data/qlib_csv_tmp")
    if tmp_csv_dir.exists():
        shutil.rmtree(tmp_csv_dir)
    tmp_csv_dir.mkdir(parents=True)
    
    print("Partitioning data by symbol into temporary CSVs...")
    # Note: official tool expects CSV files to have headers or not?
    # Usually it expects headers if we specify field names.
    for symbol, group in df.groupby('symbol'):
        group.to_csv(tmp_csv_dir / f"{symbol}.csv", index=False)
    
    # Delete existing qlib_data
    if os.path.exists(qlib_dir):
        print(f"Deleting existing qlib_dir: {qlib_dir}")
        shutil.rmtree(qlib_dir, ignore_errors=True)
    os.makedirs(qlib_dir, exist_ok=True)
    
    print(f"Running official dump_bin.py...")
    cmd = [
        sys.executable, qlib_scripts_path, "dump_all",
        "--data_path", str(tmp_csv_dir),
        "--qlib_dir", str(qlib_dir),
        "--date_field_name", "date",
        "--symbol_field_name", "symbol",
        "--exclude_fields", "symbol,date"
    ]
    
    print(f"Command: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        print("Error running official dump_bin:")
        print(result.stderr)
        print(result.stdout)
    else:
        print("Official dump_bin output:")
        print(result.stdout)
        print("Conversion finished successfully!")
    
    # Clean up
    print("Cleaning up temporary CSVs...")
    shutil.rmtree(tmp_csv_dir)

if __name__ == "__main__":
    parquet_path = "data/qlib_source/ohlcv_with_factor_2016_2026.parquet"
    qlib_dir = "db/qlib_data"
    qlib_scripts_path = "qlib-main/scripts/dump_bin.py"
    convert_with_official_tool(parquet_path, qlib_dir, qlib_scripts_path)
