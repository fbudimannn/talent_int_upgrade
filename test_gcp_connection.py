import os
from google.cloud import storage
from google.cloud import bigquery

# 1. SETUP KUNCI (AUTHENTICATION) JSON FILE
# Ganti nama file ini sesuai nama file JSON 
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "gcp-key.json"

def test_connections():
    print("--- Memulai Tes Koneksi GCP ---")
    
    try:
        # TES 1: Cek Google Cloud Storage (GCS) 
        storage_client = storage.Client()
        buckets = list(storage_client.list_buckets())
        print(f"✅ GCS Connected! Ditemukan {len(buckets)} bucket.")
        for b in buckets:
            print(f"   - Nama Bucket: {b.name}")

        # TES 2: Cek BigQuery 
        bq_client = bigquery.Client()
        datasets = list(bq_client.list_datasets())
        print(f"\n✅ BigQuery Connected! Ditemukan {len(datasets)} dataset.")
        for d in datasets:
            print(f"   - Nama Dataset: {d.dataset_id}")
            
        print("\n🎉 SEMUA AMAN! Siap untuk Ingestion.") # Success message

    except Exception as e:
        print("\n❌ TERJADI ERROR:")
        print(e)
        print("\nTips: Cek apakah file JSON benar, lokasi path benar, atau API sudah diaktifkan.")

if __name__ == "__main__":  
    test_connections() # Run the function to test the connections   