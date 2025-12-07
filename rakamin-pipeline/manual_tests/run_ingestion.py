# =====================================================================
# MANUAL INGESTION RUNNER (untuk testing local tanpa Airflow)
# =====================================================================

from plugins.pipeline.ingestion_logic import upload_csv, load_to_bigquery

print("⚡ Running manual ingestion test...")

# STEP 1: Upload CSV ke GCS
files = upload_csv()

# STEP 2: Load ke BigQuery
load_to_bigquery(files)

print("🎉 Manual ingestion completed!")
