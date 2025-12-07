# =====================================================================
# INGESTION LOGIC MODULE
# Berisi fungsi upload CSV ke GCS dan load dari GCS ke BigQuery
# Bisa dipakai Airflow DAN bisa dipakai manual run
# =====================================================================

import os
from datetime import datetime
import json

from google.cloud import storage
from google.cloud import bigquery

# ---------------------------------------------------------------------
# GLOBAL CONFIG (berlaku untuk local dan airflow)
# ---------------------------------------------------------------------

# Lokasi folder include (berisi data & credential)
BASE_PATH = os.path.join(os.getcwd(), "include")

# Lokasi file GCP key JSON (untuk local mode)
KEY_PATH = os.path.join(BASE_PATH, "gcp-key.json")

# Set environment var agar google-cloud-library tahu lokasi key
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = KEY_PATH

# GCP Configuration
PROJECT_ID = "celtic-facility-344813"
BUCKET_NAME = "rakamin-datalake-fai"
DATASET_ID = "raw_data"

# Lokasi folder CSV
DATA_FOLDER = BASE_PATH # os.path.join(BASE_PATH, "include") atau ini bisa


# =====================================================================
# FUNCTION 1: Upload all CSV files from local to Google Cloud Storage
# =====================================================================
def upload_csv():
    """
    Mengupload SEMUA file CSV di folder include/data ke GCS.
    Return:
        list of dict:
            [{"table_name": "employees", "gcs_uri": "gs://bucket/path/file"}]
    """

    print("🚀 Starting CSV Upload Process")

    # Create GCS client
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)

    # List semua CSV di include
    csv_files = [f for f in os.listdir(DATA_FOLDER) if f.endswith(".csv")]

    uploaded_info = []

    for file in csv_files:
        local_path = os.path.join(DATA_FOLDER, file)

        # Nama tabel = nama file tanpa .csv
        table_name = file.replace(".csv", "")

        # Path GCS: landing_zone/manual/YYYY-MM-DD/file.csv
        gcs_path = f"landing_zone/manual/{datetime.now().strftime('%Y-%m-%d')}/{file}"

        # Upload
        blob = bucket.blob(gcs_path)
        blob.upload_from_filename(local_path)

        print(f"   ✅ UPLOADED {file} → gs://{BUCKET_NAME}/{gcs_path}")

        # Simpan metadata untuk dipakai BigQuery
        uploaded_info.append({
            "table_name": table_name,
            "gcs_uri": f"gs://{BUCKET_NAME}/{gcs_path}"
        })

    print("🎉 Upload Complete!")
    return uploaded_info



# =====================================================================
# FUNCTION 2: Load uploaded CSVs from GCS → BigQuery
# =====================================================================
def load_to_bigquery(files_from_xcom, **context):
    print("🚀 Starting BigQuery Load")

    # MODE LOCAL → langsung list dict
    if isinstance(files_from_xcom, list):
        files = files_from_xcom

    # MODE AIRFLOW → XCom berupa STRING
    elif isinstance(files_from_xcom, str):
        print("🔄 Converting XCom string → list object")
        files = json.loads(files_from_xcom.replace("'", '"'))

    else:
        raise ValueError("Unsupported file format")

    bq_client = bigquery.Client()

    for f in files:
        table_id = f"{PROJECT_ID}.{DATASET_ID}.{f['table_name']}"
        uri = f['gcs_uri']

        print(f"⏳ Loading {uri} → {table_id}")

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            autodetect=True,
            write_disposition="WRITE_TRUNCATE",
        )

        load_job = bq_client.load_table_from_uri(uri, table_id, job_config=job_config)
        load_job.result()

        print(f"   ✅ Loaded → {table_id}")

    print("🎉 BigQuery Load Completed!")
