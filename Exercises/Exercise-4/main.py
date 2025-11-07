import glob
import json
import csv
import os

def flatten_json(data, parent_key="", sep="_"):
    items = []
    for k, v in data.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_json(v, new_key, sep=sep).items())
        elif isinstance(v, list):
            if all(isinstance(i, (int, float, str)) for i in v):
                items.append((new_key, ",".join(map(str, v))))
            else:
                for i, elem in enumerate(v):
                    if isinstance(elem, dict):
                        items.extend(flatten_json(elem, f"{new_key}{sep}{i}", sep=sep).items())
                    else:
                        items.append((f"{new_key}{sep}{i}", elem))
        else:
            items.append((new_key, v))
    return dict(items)

def convert_json_to_csv(json_path, csv_path):
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, list):
        flat_list = [flatten_json(item) for item in data]
        keys = sorted({k for d in flat_list for k in d})
        with open(csv_path, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=keys)
            writer.writeheader()
            writer.writerows(flat_list)
    else:
        flat_data = flatten_json(data)
        with open(csv_path, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=flat_data.keys())
            writer.writeheader()
            writer.writerow(flat_data)

def main():
    base_dir = "data"
    json_files = glob.glob(os.path.join(base_dir, "**/*.json"), recursive=True)
    for json_path in json_files:
        rel_path = os.path.relpath(json_path, base_dir)
        csv_path = os.path.join("output", os.path.splitext(rel_path)[0] + ".csv")
        os.makedirs(os.path.dirname(csv_path), exist_ok=True)
        convert_json_to_csv(json_path, csv_path)
    print(f"converted {len(json_files)} Json files to CSV format")

if __name__ == "__main__":
    main()