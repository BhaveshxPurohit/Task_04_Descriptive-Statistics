import csv
import json
import ast
import time
from tkinter import Tk
from tkinter.filedialog import askopenfilename
from collections import Counter
from statistics import mean, stdev

def try_parse_json(val):
    if not val or not isinstance(val, str):
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
    if not val or not isinstance(val, str):
        return False
    val = val.strip()
    if any(val.startswith(c) for c in ['{', '[', '(']):
        return True
    if any(d in val for d in [',', '|', ';']):
        return True
    return False

def detect_unpackable_columns(data):
    unpackables = []
    for col in data[0]:
        for row in data:
            val = row.get(col, '')
            if is_likely_multi_valued(val):
                unpackables.append(col)
                break
    return unpackables

def unpack_dict_column(data, col, id_cols):
    rows = []
    for row in data:
        val = try_parse_json(row.get(col, ''))
        if isinstance(val, dict):
            for k, v in val.items():
                base = {key: row[key] for key in id_cols}
                keys = k.split('_', 1)
                for i, part in enumerate(keys):
                    base[f"{col}_key{i+1}"] = part
                if isinstance(v, dict):
                    for vk, vv in v.items():
                        base[f"{col}_{vk}"] = vv
                else:
                    base[f"{col}_value"] = v
                rows.append(base)
    return rows

def unpack_list_column(data, col, id_cols):
    rows = []
    for row in data:
        val = try_parse_json(row.get(col, ''))
        if isinstance(val, (list, tuple)):
            for item in val:
                base = {key: row[key] for key in id_cols}
                base[f"{col}_value"] = item
                rows.append(base)
    return rows

def unpack_delimited_string_column(data, col, id_cols, delimiters=[',', '|', ';']):
    rows = []
    for row in data:
        raw = row.get(col, '')
        for d in delimiters:
            if d in raw:
                parts = [p.strip() for p in raw.split(d) if p.strip()]
                break
        else:
            parts = [raw.strip()] if raw.strip() else []

        for p in parts:
            base = {key: row[key] for key in id_cols}
            base[f"{col}_value"] = p
            rows.append(base)
    return rows

def get_numeric_and_non_numeric(data, exclude):
    numeric_cols = set()
    non_numeric_cols = set()

    if not data:
        return [], []

    for col in data[0]:
        if col in exclude:
            continue

        values = [row[col] for row in data if col in row and row[col] not in (None, '', 'NA')]
        if not values:
            continue

        num_like = 0
        total = 0
        has_leading_zero = False
        is_all_digit_str = True

        for v in values[:100]:
            val = str(v).strip()
            if not val:
                continue
            total += 1

            if val.isdigit():
                if val.startswith("0") and len(val) > 1:
                    has_leading_zero = True
                continue

            try:
                float(val)
                num_like += 1
            except:
                is_all_digit_str = False

        if total == 0:
            continue
        elif has_leading_zero and is_all_digit_str:
            non_numeric_cols.add(col)
        elif num_like / total >= 0.9:
            numeric_cols.add(col)
        else:
            non_numeric_cols.add(col)

    return list(numeric_cols), list(non_numeric_cols)

def print_numeric_stats(data, cols):
    print("\n-- Numeric Stats --")
    for col in cols:
        try:
            values = [float(row[col]) for row in data if row.get(col) not in (None, '', 'NA')]
            if values:
                print(f"\U0001F4CA {col}: count={len(values)}, mean={mean(values):.2f}, min={min(values)}, max={max(values)}, std={stdev(values) if len(values)>1 else 0:.2f}")
        except:
            continue

def print_non_numeric_stats(data, cols):
    print("\n-- Non-Numeric Stats --")
    for col in cols:
        values = [row[col] for row in data if row.get(col)]
        count = len(values)
        unique = len(set(values))
        top = Counter(values).most_common(1)[0] if values else (None, 0)
        print(f"\U0001F522 {col}: count={count}, unique={unique}, top={top[0]}, freq={top[1]}")

def main():
    print("📁 Please select your CSV file")
    root = Tk()
    root.withdraw()
    file_path = askopenfilename(filetypes=[("CSV files", "*.csv")])
    if not file_path:
        print("❌ No file selected. Exiting.")
        return

    with open(file_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        data = list(reader)

    if not data:
        print("❌ Empty file.")
        return

    print(f"✅ Loaded CSV with {len(data)} rows and {len(data[0])} columns")

    limit = input("🔢 How many rows to load? (Enter for all): ").strip()
    if limit.isdigit():
        data = data[:int(limit)]
        print(f"✅ Truncated to {len(data)} rows")

    print("\n📋 Available columns:", list(data[0].keys()))
    id_input = input("🔤 Enter column(s) to use as identifier (comma-separated, or press Enter to skip): ").strip()
    id_cols = [c.strip() for c in id_input.split(',') if c.strip() in data[0]]

    if id_input and not id_cols:
        print("❌ None of the entered columns are valid. Exiting.")
        return

    unpackables = detect_unpackable_columns(data)
    print(f"\n🔍 Detected unpackable columns: {unpackables}")

    all_datasets = {"main": [{k: row[k] for k in row if k not in unpackables} for row in data]}

    for col in unpackables:
        sample = next((row[col] for row in data if row.get(col)), '').strip()
        parsed = try_parse_json(sample)
        if isinstance(parsed, dict):
            unpacked = unpack_dict_column(data, col, id_cols)
        elif isinstance(parsed, (list, tuple)):
            unpacked = unpack_list_column(data, col, id_cols)
        else:
            unpacked = unpack_delimited_string_column(data, col, id_cols)

        if unpacked:
            all_datasets[col] = unpacked

    for name, dataset in all_datasets.items():
        print(f"\n📘 Stats for '{name}' dataset")
        start_time = time.time()
        numeric, non_numeric = get_numeric_and_non_numeric(dataset, exclude=id_cols)
        print_numeric_stats(dataset, numeric)
        print_non_numeric_stats(dataset, non_numeric)
        end_time = time.time()
        print(f"⏱️ Stats computed in {end_time - start_time:.2f} seconds.")

if __name__ == '__main__':
    main()
