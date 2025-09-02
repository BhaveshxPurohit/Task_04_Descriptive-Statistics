import polars as pl
import json
import ast
from tkinter import Tk
from tkinter.filedialog import askopenfilename
from collections import Counter
from statistics import mean, stdev

def try_parse_json(val):
    if val is None or not isinstance(val, str):
        return None
    val = val.strip()
    if not (val.startswith('{') or val.startswith('[') or val.startswith('(')):
        return None
    try:
        return json.loads(val)
    except Exception:
        try:
            return ast.literal_eval(val)
        except Exception:
            return None

def is_likely_multi_valued(val):
    if val is None or not isinstance(val, str):
        return False
    val = val.strip()
    if any(val.startswith(c) for c in ['{', '[', '(']):
        return True
    if any(d in val for d in [',', '|', ';']) and len(val.split()) > 1:
        return True
    return False

def detect_unpackable_columns(df):
    unpackables = []
    for col in df.columns:
        # Convert to string series safely; drop nulls
        series_str = df[col].drop_nulls().cast(pl.Utf8)
        # Apply is_likely_multi_valued on first 50 non-null values
        samples = series_str.head(50).to_list()
        if any(is_likely_multi_valued(str(v)) for v in samples):
            unpackables.append(col)
    return unpackables

def unpack_column(df, col, id_cols):
    records = []
    series = df[col].drop_nulls().cast(pl.Utf8)
    # Parse JSON/literal
    parsed_vals = [try_parse_json(v) for v in series.to_list()]

    if any(isinstance(v, dict) for v in parsed_vals):
        # Unpack dicts
        for row in df.to_dicts():
            val = try_parse_json(row.get(col, ''))
            if isinstance(val, dict):
                base = {k: row.get(k) for k in id_cols}
                for k, v in val.items():
                    entry = base.copy()
                    keys = k.split('_', 1)
                    for i, part in enumerate(keys):
                        entry[f"{col}_key{i+1}"] = part
                    if isinstance(v, dict):
                        for vk, vv in v.items():
                            entry[f"{col}_{vk}"] = vv
                    else:
                        entry[f"{col}_value"] = v
                    records.append(entry)
    elif any(isinstance(v, (list, tuple)) for v in parsed_vals):
        # Unpack lists
        for row in df.to_dicts():
            val = try_parse_json(row.get(col, ''))
            if isinstance(val, (list, tuple)):
                base = {k: row.get(k) for k in id_cols}
                for item in val:
                    entry = base.copy()
                    entry[f"{col}_value"] = item
                    records.append(entry)
    else:
        # Unpack delimited strings like "Older American, Servicemember"
        for row in df.to_dicts():
            raw = row.get(col, '')
            if not isinstance(raw, str):
                continue
            items = []
            for delim in [',', '|', ';']:
                if delim in raw:
                    items = [p.strip() for p in raw.split(delim) if p.strip()]
                    break
            else:
                if raw.strip():
                    items = [raw.strip()]
            base = {k: row.get(k) for k in id_cols}
            for item in items:
                entry = base.copy()
                entry[f"{col}_value"] = item
                records.append(entry)

    if records:
        return pl.DataFrame(records)
    else:
        return pl.DataFrame()

def print_stats(df, id_cols):
    # Numeric stats
    print("\n-- Numeric Stats --")
    numeric_cols = [col for col, dt in zip(df.columns, df.dtypes) if dt in (pl.Int8, pl.Int16, pl.Int32, pl.Int64, pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64, pl.Float32, pl.Float64) and col not in id_cols]
    if numeric_cols:
        descr = df.select(numeric_cols).describe()
        print(descr)

    # Non-numeric stats
    print("\n-- Non-Numeric Stats --")
    for col, dt in zip(df.columns, df.dtypes):
        if col in id_cols or dt in (pl.Int8, pl.Int16, pl.Int32, pl.Int64, pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64, pl.Float32, pl.Float64):
            continue
        s = df[col].drop_nulls()
        values = s.to_list()
        if not values:
            continue
        counter = Counter(values)
        top_val, freq = counter.most_common(1)[0]
        print(f"🔠 {col}: count={len(values)}, unique={len(counter)}, top={top_val}, freq={freq}")

def main():
    print("📁 Please select your CSV file")
    root = Tk()
    root.withdraw()
    file_path = askopenfilename(filetypes=[("CSV files", "*.csv")])
    if not file_path:
        print("❌ No file selected. Exiting.")
        return

    try:
        df = pl.read_csv(file_path, infer_schema_length=10000, ignore_errors=True)
    except Exception as e:
        print("❌ Failed to read CSV:", e)
        return

    print(f"✅ Loaded CSV with shape: {df.shape}")

    limit = input("🔢 How many rows to load? (Enter for all): ").strip()
    if limit.isdigit():
        df = df.head(int(limit))
        print(f"✅ Truncated to {df.height} rows")

    print("\n📋 Available columns:", df.columns)
    id_input = input("🔠 Enter column(s) to use as identifier (comma-separated): ").strip()
    id_cols = [c.strip() for c in id_input.split(',') if c.strip() in df.columns]
    if not id_cols:
        print("❌ No valid identifier columns. Exiting.")
        return

    unpackables = detect_unpackable_columns(df)
    print(f"\n🔍 Detected unpackable columns: {unpackables}")

    # Include 'tags' forcibly if it contains delimiters even if missed (common issue)
    if 'tags' in df.columns and 'tags' not in unpackables:
        tags_sample = df['tags'].drop_nulls().cast(pl.Utf8).head(50).to_list()
        if any(',' in str(v) or '|' in str(v) or ';' in str(v) for v in tags_sample):
            print("🔍 Adding 'tags' column to unpackable due to delimiter detection")
            unpackables.append('tags')

    main_df = df.drop(unpackables)
    all_dfs = {"main": main_df}

    for col in unpackables:
        unpacked_df = unpack_column(df, col, id_cols)
        if unpacked_df.height > 0:
            all_dfs[col] = unpacked_df

    for name, dframe in all_dfs.items():
        print(f"\n📘 Stats for '{name}' DataFrame:")
        print_stats(dframe, id_cols)

if __name__ == "__main__":
    main()
