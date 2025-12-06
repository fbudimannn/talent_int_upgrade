# talent_int_upgrade

# 🛠️ End-to-End Data Pipeline Setup Guide

This document outlines the infrastructure setup required to run the Talent Intelligence data pipeline. The stack includes **Google Cloud Platform (GCP)** for storage and compute, **Apache Airflow (via Astro CLI)** for orchestration, and **dbt** for transformation.

## 1. Google Cloud Platform (GCP) Setup

Before running any code, the cloud infrastructure must be provisioned.

### A. Project & APIs
1.  Create a new GCP Project (e.g., `rakamin-talent-intelligence`).
2.  Enable the following APIs in **APIs & Services > Library**:
    * **Google Cloud Storage JSON API** (For Data Lake)
    * **BigQuery API** (For Data Warehouse)

### B. IAM & Security (Service Account)
1.  Navigate to **IAM & Admin > Service Accounts**.
2.  Create a new Service Account (e.g., `data-pipeline-sa`).
3.  Grant the following roles:
    * `Storage Admin` (To manage buckets/files)
    * `BigQuery Admin` (To manage datasets/tables)
4.  Generate a Key:
    * Click the Service Account > **Keys** > **Add Key** > **Create new key** (JSON).
    * **Download** the JSON file and rename it to `gcp-key.json`.
    * **IMPORTANT:** Move this file to your project root and add it to `.gitignore`. Never upload this key!

### C. Storage Resources
1.  **Cloud Storage (GCS):**
    * Create a bucket (e.g., `rakamin-raw-datalake-[name]`).
    * Region: `us-central1` (or `asia-southeast2`).
    * Class: Standard.
2.  **BigQuery:**
    * Create a dataset named `raw_data`.
    * Location: Must match the GCS bucket region.

---

## 2. Local Environment Setup

### A. Prerequisites
Ensure the following are installed on your machine:
* [Docker Desktop](https://www.docker.com/products/docker-desktop/) (Must be running)
* [Astro CLI](https://docs.astronomer.io/astro/cli/install-cli) (For running Airflow)
* Python 3.9+

### B. Airflow Initialization (Astro)
1.  Create a directory for the pipeline:
    ```bash
    mkdir rakamin-pipeline
    cd rakamin-pipeline
    ```
2.  Initialize the Astro project:
    ```bash
    astro dev init
    ```
3.  **Connect GCP Key:**
    * Copy your `gcp-key.json` into the `rakamin-pipeline` folder.
    * Ensure `gcp-key.json` is listed in `.gitignore`.

### C. Install Dependencies
Modify the `requirements.txt` file inside `rakamin-pipeline/` to include necessary libraries:

```text
# GCP Providers & Utilities
apache-airflow-providers-google
pandas
db-dtypes

# Transformation Tool
dbt-bigquery
D. Configure Dockerfile
Ensure your Dockerfile uses the correct runtime image:

Dockerfile

FROM quay.io/astronomer/astro-runtime:11.3.0

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
3. How to Run
Start Docker Desktop.

Open a terminal in the rakamin-pipeline folder.

Start the Airflow environment:

Bash

astro dev start
Access the Airflow UI:

URL: http://localhost:8080

User: admin

Pass: admin

Setup Connection in Airflow UI:

Go to Admin > Connections.

Add new connection: google_cloud_default.

Type: Google Cloud.

Keyfile JSON: Paste the content of your gcp-key.json here.

4. Project Structure
Plaintext

rakamin-pipeline/
├── dags/                 # Python scripts for Airflow DAGs
├── dbt_project/          # dbt transformations (if separated)
├── plugins/              # Custom plugins
├── Dockerfile            # Astro configuration
├── packages.txt          # OS-level dependencies
├── requirements.txt      # Python libraries
└── gcp-key.json          

