import pandas as pd
import json
import ast
import time
from tkinter import Tk
from tkinter.filedialog import askopenfilename
from collections import Counter

def try_parse_json(val):
    if pd.isna(val) or not isinstance(val, str):
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
    if pd.isna(val) or not isinstance(val, str):
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
        if df[col].dropna().astype(str).apply(is_likely_multi_valued).any():
            unpackables.append(col)
    return unpackables

def unpack_column(df, col, id_cols):
    parsed = df[col].dropna().apply(try_parse_json)
    if parsed.apply(lambda x: isinstance(x, dict)).any():
        records = []
        for _, row in df.dropna(subset=[col]).iterrows():
            base = row[id_cols].to_dict() if id_cols else {}
            val = try_parse_json(row[col])
            if isinstance(val, dict):
                for k, v in val.items():
                    entry = base.copy()
                    parts = k.split('_', 1)
                    for i, part in enumerate(parts):
                        entry[f"{col}_key{i+1}"] = part
                    if isinstance(v, dict):
                        for vk, vv in v.items():
                            entry[f"{col}_{vk}"] = vv
                    else:
                        entry[f"{col}_value"] = v
                    records.append(entry)
        return pd.DataFrame(records)

    elif parsed.apply(lambda x: isinstance(x, (list, tuple))).any():
        records = []
        for _, row in df.dropna(subset=[col]).iterrows():
            val = try_parse_json(row[col])
            if isinstance(val, (list, tuple)):
                for item in val:
                    entry = row[id_cols].to_dict() if id_cols else {}
                    entry[f"{col}_value"] = item
                    records.append(entry)
        return pd.DataFrame(records)

    else:
        records = []
        for _, row in df.dropna(subset=[col]).iterrows():
            raw = row[col]
            for delim in [',', '|', ';']:
                if delim in raw:
                    items = [p.strip() for p in raw.split(delim) if p.strip()]
                    break
            else:
                items = [raw.strip()] if raw.strip() else []
            for item in items:
                entry = row[id_cols].to_dict() if id_cols else {}
                entry[f"{col}_value"] = item
                records.append(entry)
        return pd.DataFrame(records)

def print_stats(df, id_cols):
    start_time = time.time()

    print("\n-- Numeric Stats --")
    numeric = df.select_dtypes(include='number').drop(columns=id_cols, errors='ignore')
    if not numeric.empty:
        print(numeric.describe().T.to_string())

    print("\n-- Non-Numeric Stats --")
    for col in df.select_dtypes(include='object').columns:
        if col in id_cols:
            continue
        s = df[col].dropna()
        top_val = s.mode().iloc[0] if not s.mode().empty else None
        freq = (s == top_val).sum() if top_val else 0
        print(f"🔠 {col}: count={s.count()}, unique={s.nunique()}, top={top_val}, freq={freq}")

    end_time = time.time()
    print(f"\n⏱️ Stats computed in {end_time - start_time:.2f} seconds")

def main():
    print("📁 Please select your CSV file")
    root = Tk()
    root.withdraw()
    file_path = askopenfilename(filetypes=[("CSV files", "*.csv")])
    if not file_path:
        print("❌ No file selected. Exiting.")
        return

    df = pd.read_csv(file_path)
    print(f"✅ Loaded CSV with shape: {df.shape}")

    limit = input("🔢 How many rows to load? (Enter for all): ").strip()
    if limit.isdigit():
        df = df.head(int(limit))
        print(f"✅ Truncated to {df.shape[0]} rows")

    print("\n📋 Available columns:", list(df.columns))
    id_input = input("🔠 Enter column(s) to use as identifier (comma-separated, or press Enter to skip): ").strip()
    id_cols = [c.strip() for c in id_input.split(',') if c.strip() in df.columns]

    if id_input and not id_cols:
        print("❌ None of the entered columns are valid. Exiting.")
        return

    unpackables = detect_unpackable_columns(df)
    print(f"\n🔍 Detected unpackable columns: {unpackables}")

    all_datasets = {"main": df.drop(columns=unpackables, errors='ignore')}

    for col in unpackables:
        unpacked_df = unpack_column(df, col, id_cols)
        if not unpacked_df.empty:
            all_datasets[col] = unpacked_df

    for name, subset in all_datasets.items():
        print(f"\n📘 Stats for '{name}' DataFrame:")
        print_stats(subset, id_cols)

if __name__ == '__main__':
    main()
