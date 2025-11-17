# src/cleaning/data_cleaning.py
import os
import sys
import pandas as pd
from datetime import timedelta
from databricks.connect import DatabricksSession

def get_spark():
    """
    Build Databricks Connect remote Spark session.
    Requires the following env vars set:
      - DATABRICKS_HOST
      - DATABRICKS_TOKEN
      - DATABRICKS_CLUSTER_ID
      - DATABRICKS_HTTP_PATH
    """
    host = os.getenv("DATABRICKS_HOST")
    token = os.getenv("DATABRICKS_TOKEN")
    cluster_id = os.getenv("DATABRICKS_CLUSTER_ID")
    http_path = os.getenv("DATABRICKS_HTTP_PATH")

    if not all([host, token, cluster_id, http_path]):
        raise RuntimeError("Missing one of DATABRICKS_HOST/DATABRICKS_TOKEN/DATABRICKS_CLUSTER_ID/DATABRICKS_HTTP_PATH")

    return (
        DatabricksSession.builder
        .host(host)
        .token(token)
        .cluster_id(cluster_id)
        .http_path(http_path)
        .getOrCreate()
    )

def main():
    # table names (catalog.schema.table). Adjust if needed.
    CATALOG = os.getenv("DATABRICKS_CATALOG", "workspace")
    SCHEMA = os.getenv("DATABRICKS_SCHEMA", "feature_store_project")
    RAW_TABLE = f"{CATALOG}.{SCHEMA}.fct_claim"
    CLEAN_TABLE = f"{CATALOG}.{SCHEMA}.cleaned_claims"
    FEATURE_TABLE = f"{CATALOG}.{SCHEMA}.fct_claim_features_after_datacleanup"

    spark = get_spark()
    print("Connected to Databricks via Databricks Connect")

    # read source table using spark (Delta)
    print(f"Reading source table: {RAW_TABLE}")
    spark_df = spark.read.table(RAW_TABLE)
    row_count = spark_df.count()
    print(f"Rows in source: {row_count}, columns: {len(spark_df.columns)}")

    # For complex cleaning it's convenient to convert to pandas (only if the data fits in driver memory)
    # If table is large, you should perform cleaning using Spark operations instead.
    print("Converting Spark DataFrame to Pandas (ensure the dataset fits in memory)")
    df = spark_df.toPandas()
    print("Pandas shape:", df.shape)

    # -----------------------
    # Data cleaning (your logic)
    # -----------------------
    # drop rows that are all null
    df.dropna(how="all", inplace=True)

    # drop columns with >40% missing
    missing_pct = df.isnull().mean() * 100
    cols_to_drop = missing_pct[missing_pct > 40].index.tolist()
    if cols_to_drop:
        print("Dropping columns with >40% missing:", cols_to_drop)
    df.drop(columns=cols_to_drop, inplace=True, errors="ignore")

    # drop unique id if not useful
    df.drop(columns=["authzn_id"], errors="ignore", inplace=True)

    # convert and impute dates
    for col in ["pd_dt", "clm_adjud_ts", "clm_sys_cret_dt"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    if "clm_sys_cret_dt" in df.columns and "pd_dt" in df.columns:
        df["pd_dt"] = df["pd_dt"].fillna(df["clm_sys_cret_dt"] + pd.Timedelta(days=2))

    if "pd_dt" in df.columns and "clm_adjud_ts" in df.columns:
        df["clm_adjud_ts"] = df["clm_adjud_ts"].fillna(df["pd_dt"] - pd.Timedelta(days=2))

    # categorical missing
    if "procsr_apprv_pay_by_nm" in df.columns:
        df["procsr_apprv_pay_by_nm"] = df["procsr_apprv_pay_by_nm"].fillna("unknown")

    # outlier removal on totl_billd_amt
    if "totl_billd_amt" in df.columns:
        Q1 = df["totl_billd_amt"].quantile(0.25)
        Q3 = df["totl_billd_amt"].quantile(0.75)
        IQR = Q3 - Q1
        lower, upper = Q1 - 1.5 * IQR, Q3 + 1.5 * IQR
        before = df.shape[0]
        df = df[(df["totl_billd_amt"] >= lower) & (df["totl_billd_amt"] <= upper)]
        after = df.shape[0]
        print(f"Removed outliers from totl_billd_amt: {before - after} rows removed")

    # drop duplicates
    before_dup = df.shape[0]
    df.drop_duplicates(inplace=True)
    print(f"Dropped {before_dup - df.shape[0]} duplicate rows")

    print("After cleaning shape:", df.shape)

    # -----------------------
    # Write cleaned back to Delta using Spark
    # -----------------------
    print("Converting cleaned pandas DataFrame back to Spark DataFrame")
    spark_clean = spark.createDataFrame(df)

    print(f"Writing cleaned table (overwrite): {CLEAN_TABLE}")
    spark_clean.write.mode("overwrite").format("delta").saveAsTable(CLEAN_TABLE)
    print("Cleaned table written.")

    # Optionally create a features table (here we just write the same cleaned table as feature table)
    # If you want to maintain separate feature-store semantics, do it in a later stage.
    print(f"Creating/Overwriting feature table: {FEATURE_TABLE}")
    spark_clean.write.mode("overwrite").format("delta").saveAsTable(FEATURE_TABLE)
    print("Feature table written.")

    spark.stop()
    print("Data cleaning completed successfully.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("ERROR:", e, file=sys.stderr)
        raise
#attempt 2