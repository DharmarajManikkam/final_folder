# Fraud_detection1 - CI with Databricks Connect

## Quick start
1. Add GitHub repo secrets:
   - DATABRICKS_HOST
   - DATABRICKS_TOKEN
   - DATABRICKS_CLUSTER_ID
   - DATABRICKS_HTTP_PATH

2. Ensure the specified Databricks cluster is running.

3. Push to `main` branch — CI will run automatically and execute `src/cleaning/data_cleaning.py`.

## Files
- src/cleaning/data_cleaning.py : cleaning logic (Databricks Connect)
- config/databricks_config.yaml : generated during CI (do not store secrets here)
- .github/workflows/ci.yml : GitHub Actions pipeline
- requirements.txt : Python deps
