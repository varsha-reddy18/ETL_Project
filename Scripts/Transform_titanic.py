# transform_titanic.py
import os
import pandas as pd
from Extract_titanic import extract_data

def transform_data(raw_path: str):
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    staged_dir = os.path.join(base_dir, "data", "staged")
    os.makedirs(staged_dir, exist_ok=True)
    df = pd.read_csv(raw_path)
    numeric_cols = ["age", "sibsp", "parch", "fare"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = df[col].fillna(df[col].median())
    cat_cols = ["sex", "embarked", "class", "who", "embark_town", "alive"]
    for col in cat_cols:
        if col in df.columns:
            df[col] = df[col].fillna(df[col].mode()[0])
    if set(["sibsp", "parch"]).issubset(df.columns):
        df["family_size"] = df["sibsp"] + df["parch"] + 1
    if "fare" in df.columns and "family_size" in df.columns:
        df["fare_per_person"] = df["fare"] / df["family_size"]
    if "age" in df.columns:
        df["is_child"] = (df["age"] < 18).astype(int)
    if "sex" in df.columns:
        df["is_female"] = (df["sex"] == "female").astype(int)
    cols_to_drop = ["deck"]
    df.drop(columns=[c for c in cols_to_drop if c in df.columns],
            inplace=True,
            errors="ignore")
    staged_path = os.path.join(staged_dir, "titanic_transformed.csv")
    df.to_csv(staged_path, index=False)
    print(f"Data transformed and saved at: {staged_path}")
    return staged_path
if __name__ == "__main__":
    raw_path = extract_data()
    transform_data(raw_path)
