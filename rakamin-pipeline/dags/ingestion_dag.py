from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import pandas as pd
from google.cloud import storage, bigquery

# ==========================================
# 1. KONFIGURASI GLOBAL (SETUP)
# ==========================================

# --- DETEKSI LINGKUNGAN (DOCKER vs LOCAL) ---
# Airflow di Docker Astro selalu punya env variable 'AIRFLOW_HOME' (/usr/local/airflow)
# Kalau tidak ada, berarti kita sedang di laptop (Local Manual Mode)

if os.getenv("AIRFLOW_HOME"): 
    # Path di dalam Docker
    BASE_PATH = "/usr/local/airflow/include" 
    print("🤖 Mode: Running inside Airflow (Docker Container)")
else:
    # Path di Laptop Kamu (Folder rakamin-pipeline saat ini)
    BASE_PATH = os.path.join(os.getcwd(), "include")
    print(f"💻 Mode: Running Manual (Local Laptop) at {BASE_PATH}")

# --- LOKASI FILE PENTING ---
# Pastikan file gcp-key.json dan folder data ada di dalam folder rakamin-pipeline!
KEY_PATH = os.path.join(BASE_PATH, "gcp-key.json")
DATA_PATH = BASE_PATH
# # Asumsi: File CSV ada di dalam folder 'data' YANG ADA DI DALAM 'include'
# # Jadi path-nya: .../include/data/
# DATA_PATH = os.path.join(BASE_PATH, "data")

# --- KONFIGURASI GCP ---
# Set Variable Environment supaya library Google bisa baca kuncinya
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = KEY_PATH

# Ganti dengan Project ID kamu yang sebenarnya!
PROJECT_ID = "celtic-facility-344813" 
BUCKET_NAME = "rakamin-datalake-fai" 
DATASET_ID = "raw_data" 

# ==========================================
# 2. FUNGSI UTAMA (LOGIC ETL)
# ==========================================

def upload_csv_to_gcs(**kwargs):
    """
    Tugas 1: Upload semua file CSV dari folder 'data/' ke GCS Bucket.
    Mengembalikan daftar file yang berhasil diupload (untuk dipakai tugas selanjutnya).
    """
    print(f"🚀 [Task 1] Memulai Upload ke GCS Bucket: {BUCKET_NAME}...")
    
    try:
        # Cek apakah kunci ada
        if not os.path.exists(KEY_PATH):
            raise FileNotFoundError(f"❌ Kunci GCP tidak ditemukan di: {KEY_PATH}")

        # Cek apakah folder data ada
        if not os.path.exists(DATA_PATH):
            raise FileNotFoundError(f"❌ Folder data tidak ditemukan di: {DATA_PATH}")

        # Inisialisasi Client GCS
        storage_client = storage.Client()
        bucket = storage_client.bucket(BUCKET_NAME)
        
        # Ambil semua file berakhiran .csv
        files = [f for f in os.listdir(DATA_PATH) if f.endswith('.csv')]
        
        if not files:
            print("⚠️ Tidak ada file CSV di folder data.")
            return []

        uploaded_files_info = [] # List untuk menyimpan info file yang sukses

        for filename in files:
            local_file_path = os.path.join(DATA_PATH, filename)
            
            # Tentukan alamat tujuan di GCS (Folder berdasarkan Tanggal Hari Ini)
            # Contoh: landing_zone/manual/2023-10-27/employees.csv
            blob_path = f"landing_zone/manual/{datetime.now().strftime('%Y-%m-%d')}/{filename}"
            
            # Proses Upload
            blob = bucket.blob(blob_path)
            blob.upload_from_filename(local_file_path)
            
            print(f"   ✅ Uploaded: {filename} -> gs://{BUCKET_NAME}/{blob_path}")
            
            # Simpan info penting (Nama Tabel & Lokasi GCS) ke list
            uploaded_files_info.append({
                "table_name": filename.replace('.csv', ''), # Hapus .csv buat jadi nama tabel
                "gcs_uri": f"gs://{BUCKET_NAME}/{blob_path}"
            })
            
        print(f"🎉 Total {len(uploaded_files_info)} file berhasil diupload.")
        return uploaded_files_info # Return list ini agar bisa ditangkap oleh XCom (Airflow)

    except Exception as e:
        print(f"❌ CRITICAL ERROR di Upload: {e}")
        raise e # PENTING: Lempar error biar Task Airflow jadi MERAH (Failed)

def load_gcs_to_bq(files_to_load=None, **kwargs):
    """
    Tugas 2: Mengambil file dari GCS dan memuatnya ke tabel BigQuery.
    Bisa menerima input manual (list) atau otomatis dari Airflow (XCom).
    """
    print("🚀 [Task 2] Memulai Load ke BigQuery...")
    
    try:
        # --- HYBRID LOGIC (SWITCH ON/OFF) ---
        # Jika 'files_to_load' kosong (berarti jalan via Airflow), ambil dari XCom
        if files_to_load is None:
            print("📡 Mode Airflow: Mengambil daftar file dari XCom (Task sebelumnya)...")
            ti = kwargs['ti'] # Task Instance
            files_to_load = ti.xcom_pull(task_ids='task_upload_to_gcs')
        else:
            print("💻 Mode Manual: Menggunakan daftar file dari argumen fungsi.")

        # Validasi jika tidak ada data
        if not files_to_load:
            print("⚠️ Peringatan: Tidak ada file untuk diproses. Pipeline berhenti.")
            return

        # Inisialisasi Client BigQuery
        bq_client = bigquery.Client()
        
        for file_info in files_to_load:
            # Tentukan Nama Tabel Tujuan (Project.Dataset.Table)
            table_id = f"{PROJECT_ID}.{DATASET_ID}.{file_info['table_name']}"
            uri = file_info['gcs_uri']
            
            # Konfigurasi Job Load
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.CSV,
                skip_leading_rows=1,            # Lewati header CSV
                autodetect=True,                # Otomatis deteksi tipe data
                write_disposition="WRITE_TRUNCATE", # HAPUS data lama, ISI dengan yang baru
            )

            print(f"⏳ Loading: {uri} -> Tabel: {table_id}...")
            
            load_job = bq_client.load_table_from_uri(uri, table_id, job_config=job_config)
            load_job.result() # Tunggu sampai selesai (Blocking)
            
            # Cek hasil
            destination_table = bq_client.get_table(table_id)
            print(f"   ✅ Sukses! Tabel {table_id} sekarang punya {destination_table.num_rows} baris.")
            
    except Exception as e:
        print(f"❌ CRITICAL ERROR di Load BigQuery: {e}")
        raise e # PENTING: Lempar error biar Task Airflow jadi MERAH (Failed)

# ==========================================
# 3. DEFINISI DAG AIRFLOW (ORCHESTRATION)
# ==========================================
# Bagian ini HANYA akan dibaca oleh Airflow Server.

default_args = {
    'owner': 'rakamin_data_team',
    'retries': 0, # Gak usah retry dulu biar cepet debug
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='rakamin_ingestion_hybrid',        # Nama unik DAG di dashboard Airflow
    default_args=default_args,
    description='Pipeline Ingestion CSV Lokal ke BigQuery (Hybrid Mode)',
    start_date=datetime(2023, 1, 1),          # Tanggal mulai
    schedule_interval='@daily',               # Jalan otomatis tiap jam 00:00
    catchup=False,                            # Jangan jalankan job masa lalu
    tags=['ingestion', 'gcp', 'rakamin'],
) as dag:

    # Definisi Task 1: Upload
    t1 = PythonOperator(
        task_id='task_upload_to_gcs',
        python_callable=upload_csv_to_gcs,
        provide_context=True # Wajib True biar bisa kirim data (XCom)
    )

    # Definisi Task 2: Load
    t2 = PythonOperator(
        task_id='task_load_to_bq',
        python_callable=load_gcs_to_bq,
        provide_context=True # Wajib True biar bisa baca data (XCom)
    )

    # Atur Urutan: t1 selesai baru t2 jalan
    t1 >> t2

# ==========================================
# 4. MODE MANUAL (SWITCH ON/OFF)
# ==========================================
# Bagian ini HANYA jalan kalau kamu ketik "python ingestion_dag.py" di terminal.

if __name__ == "__main__":
    print("\n⚡⚡ MEMULAI MODE TESTING MANUAL (TANPA AIRFLOW) ⚡⚡")
    
    # 1. Jalankan Fungsi Upload secara langsung
    try:
        hasil_upload = upload_csv_to_gcs()
        
        # 2. Jalankan Fungsi Load secara langsung
        # Kita oper 'hasil_upload' secara manual ke fungsi kedua
        if hasil_upload:
            load_gcs_to_bq(files_to_load=hasil_upload)
        
        print("\n🎉 Selesai! Cek BigQuery kamu untuk verifikasi.")
    except Exception as e:
        print(f"\n💥 Program Berhenti karena Error: {e}")