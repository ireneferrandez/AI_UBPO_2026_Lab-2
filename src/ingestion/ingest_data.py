import json
import csv
import os

CURRENT_FILE = os.path.abspath(__file__)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(CURRENT_FILE)))

DATASET_PATH = os.path.join(BASE_DIR, "data", "raw", "orbital_observations.csv")
METADATA_PATH = os.path.join(BASE_DIR, "data", "raw", "metdata.json")
PROCESSED_DIR = os.path.join(BASE_DIR, "data", "processed")
VALID_FILE = os.path.join(PROCESSED_DIR, "observations_valid.csv")
INVALID_FILE = os.path.join(PROCESSED_DIR, "observations_invalid.csv")
MODEL_INPUT_FILE = os.path.join(PROCESSED_DIR, "model_input.csv")


def load_metadata(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_dataset(path):
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        columns = reader.fieldnames
    return rows, columns


def validate_columns(dataset_columns, metadata_columns):
    if dataset_columns == metadata_columns:
        print("Column validation: OK")
    else:
        print("Column validation: MISMATCH")
        print(f"Expected: {metadata_columns}")
        print(f"Actual:   {dataset_columns}")


def validate_record_count(rows, expected_count):
    actual_count = len(rows)
    if actual_count == expected_count:
        print("Record count: OK")
    else:
        print("Record count: MISMATCH")
        print(f"Expected: {expected_count}")
        print(f"Actual:   {actual_count}")


def detect_invalid_records(rows, invalid_value="INVALID", check_column="temperature"):
    valid_records = []
    invalid_records = []
    for row in rows:
        value = row.get(check_column, "").strip()
        if value.upper() == invalid_value:
            invalid_records.append(row)
        else:
            valid_records.append(row)
    print(f"Valid records: {len(valid_records)}")
    print(f"Invalid records: {len(invalid_records)}")
    return valid_records, invalid_records


def save_records(path, rows, columns):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)


def prepare_model_input(valid_records, feature_columns):
    model_input = []
    for row in valid_records:
        filtered_row = {col: row[col] for col in feature_columns if col in row}
        model_input.append(filtered_row)
    return model_input

def check_metadata_consistency(dataset_columns, feature_columns, target_column):
    missing_features = [col for col in feature_columns if col not in dataset_columns]
    target_missing = target_column not in dataset_columns
    return missing_features, target_missing


def save_summary(file_path, summary_text):
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(summary_text)


def main():
    print("Metadata path:", METADATA_PATH)
    print("Dataset path:", DATASET_PATH)

    metadata = load_metadata(METADATA_PATH)
    rows, dataset_columns = load_dataset(DATASET_PATH)
    metadata_columns = metadata.get("columns", [])
    expected_records = metadata.get("num_records", None)
    feature_columns = metadata.get("feature_columns", [])
    target_column = metadata.get("target_column", "")
	
    print(f"Dataset: {metadata.get('dataset_name', 'unknown')}")
    print(f"Records loaded: {len(rows)}")
    print(f"Columns (dataset): {dataset_columns}")
    print(f"Columns (metadata): {metadata_columns}")

    validate_columns(dataset_columns, metadata_columns)

    if expected_records is not None:
        validate_record_count(rows, expected_records)

    valid_records, invalid_records = detect_invalid_records(
        rows, invalid_value="INVALID", check_column="temperature"
    )

    save_records(VALID_FILE, valid_records, dataset_columns)
    save_records(INVALID_FILE, invalid_records, dataset_columns)

    expected_invalid = metadata.get("num_invalid_records")
    if expected_invalid is not None:
        if len(invalid_records) == expected_invalid:
            print("Invalid record count matches metadata")
        else:
            print("Invalid record count mismatch")
            print(f"Expected: {expected_invalid}")
            print(f"Actual:   {len(invalid_records)}")

    model_input_data = prepare_model_input(valid_records, feature_columns)
    save_records(MODEL_INPUT_FILE, model_input_data, feature_columns)
    
    
    column_validation_result = validate_columns(dataset_columns, metadata_columns)

    if expected_records is not None:
        record_count_result, actual_count = validate_record_count(rows, expected_records)
    else:
        record_count_result = "N/A"
        actual_count = len(rows)

    valid_records, invalid_records = detect_invalid_records(
        rows, invalid_value="INVALID", check_column="temperature"
    )

    save_records(VALID_FILE, valid_records, dataset_columns)
    save_records(INVALID_FILE, invalid_records, dataset_columns)

    model_input_data = prepare_model_input(valid_records, feature_columns)
    save_records(MODEL_INPUT_FILE, model_input_data, feature_columns)

    missing_features, target_missing = check_metadata_consistency(
        dataset_columns, feature_columns, target_column
    )

    if missing_features:
        feature_validation_result = f"Missing feature columns: {missing_features}"
    else:
        feature_validation_result = "Feature validation: OK"

    target_validation_result = "Target validation: OK" if not target_missing else f"Target column missing: {target_column}"

    summary_lines = [
        f"Dataset: {metadata.get('dataset_name', 'unknown')}",
        f"Records loaded: {len(rows)}",
        f"Expected records: {expected_records}",
        f"Column validation: {column_validation_result}",
        f"Record count validation: {record_count_result}",
        f"Valid records: {len(valid_records)}",
        f"Invalid records: {len(invalid_records)}",
        feature_validation_result,
        target_validation_result,
        "Generated files:",
        f"- {VALID_FILE}",
        f"- {INVALID_FILE}",
        f"- {MODEL_INPUT_FILE}"
    ]
    summary_text = "\n".join(summary_lines)
    save_summary(SUMMARY_FILE, summary_text)
    print(f"Ingestion summary saved to: {SUMMARY_FILE}")

	

if __name__ == "__main__":
    main()
