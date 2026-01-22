import pandas as pd
import json

RAW_FILE = "/opt/airflow/data/staging/api_data.json"
CLEAN_FILE = "/opt/airflow/data/staging/api_clean.csv"

def clean_api_data():
    # load raw JSON
    with open(RAW_FILE, "r") as f:
        data = json.load(f)

    df = pd.json_normalize(data)

    # ---------- Cleaning Rules ----------
    # remove rows with missing ID
    df = df.dropna(subset=["id"])

    # standardize text columns
    if "title" in df.columns:
        df["title"] = df["title"].str.title()

    if "category" in df.columns:
        df["category"] = df["category"].str.title()

    # fill null numeric values
    numeric_cols = df.select_dtypes(include="number").columns
    df[numeric_cols] = df[numeric_cols].fillna(0)

    # drop duplicates
    df = df.drop_duplicates()

    # save cleaned csv
    df.to_csv(CLEAN_FILE, index=False)

    print(f"Cleaned file saved to: {CLEAN_FILE}")
    print(f"Rows after cleaning: {len(df)}")


if __name__ == "__main__":
    clean_api_data()
