"""
DAG 2: daily_to_gcs_bq_dag
- 스케줄: 매일 새벽 1시 (0 1 * * *)
- 역할:
    1. 전날 로컬 CSV 24개 읽기
    2. Parquet 변환
    3. GCS 업로드 (gs://BUCKET/daily/YYYY-MM-DD.parquet)
    4. 로컬 CSV 삭제
    5. BigQuery Load Job (GCS → BQ)

Airflow Variables:
  - GCP_SERVICE_ACCOUNT : GCP 서비스 계정 JSON 문자열
  - GCP_PROJECT_ID      : GCP 프로젝트 ID
  - GCS_BUCKET          : GCS 버킷 이름
"""

import json
import os
import logging
from datetime import datetime

import pandas as pd
from google.cloud import bigquery, storage
from google.oauth2 import service_account

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator


LOCAL_DATA_DIR = "/opt/airflow/data/air_quality"
BQ_DATASET     = "air_quality"
BQ_TABLE       = "realtime_measurements"


def _get_credentials():
    json_str = Variable.get("GCP_SERVICE_ACCOUNT")
    service_info = json.loads(json_str)
    return service_account.Credentials.from_service_account_info(service_info)


def convert_to_parquet(**context):
    execution_time = context["execution_date"]
    yesterday = execution_time.strftime("%Y-%m-%d")   # timedelta 제거

    csv_dir = os.path.join(LOCAL_DATA_DIR, yesterday)

    if not os.path.exists(csv_dir):
        raise FileNotFoundError(f"CSV 폴더 없음: {csv_dir}")

    csv_files = sorted([
        os.path.join(csv_dir, f)
        for f in os.listdir(csv_dir)
        if f.endswith(".csv")
    ])

    if not csv_files:
        raise FileNotFoundError(f"CSV 파일 없음: {csv_dir}")

    logging.info(f"CSV 파일 {len(csv_files)}개 병합 시작: {yesterday}")

    df = pd.concat([pd.read_csv(f) for f in csv_files], ignore_index=True)

    parquet_path = os.path.join(LOCAL_DATA_DIR, f"{yesterday}.parquet")
    df.to_parquet(parquet_path, index=False)

    logging.info(f"Parquet 변환 완료: {parquet_path} ({len(df)}행)")

    context["ti"].xcom_push(key="parquet_path", value=parquet_path)
    context["ti"].xcom_push(key="yesterday", value=yesterday)


def upload_to_gcs(**context):
    ti           = context["ti"]
    parquet_path = ti.xcom_pull(key="parquet_path", task_ids="convert_to_parquet")
    yesterday    = ti.xcom_pull(key="yesterday",    task_ids="convert_to_parquet")

    bucket_name  = Variable.get("GCS_BUCKET")
    credentials  = _get_credentials()

    client    = storage.Client(credentials=credentials)
    bucket    = client.bucket(bucket_name)
    blob_name = f"daily/{yesterday}.parquet"
    blob      = bucket.blob(blob_name)

    blob.upload_from_filename(parquet_path)

    gcs_path = f"gs://{bucket_name}/{blob_name}"
    logging.info(f"GCS 업로드 완료: {gcs_path}")

    context["ti"].xcom_push(key="gcs_path", value=gcs_path)


def delete_local_files(**context):
    import shutil

    ti           = context["ti"]
    parquet_path = ti.xcom_pull(key="parquet_path", task_ids="convert_to_parquet")
    yesterday    = ti.xcom_pull(key="yesterday",    task_ids="convert_to_parquet")

    csv_dir = os.path.join(LOCAL_DATA_DIR, yesterday)

    if os.path.exists(csv_dir):
        shutil.rmtree(csv_dir)
        logging.info(f"로컬 CSV 폴더 삭제 완료: {csv_dir}")

    if os.path.exists(parquet_path):
        os.remove(parquet_path)
        logging.info(f"로컬 Parquet 파일 삭제 완료: {parquet_path}")


def load_to_bigquery(**context):
    ti       = context["ti"]
    gcs_path = ti.xcom_pull(key="gcs_path", task_ids="upload_to_gcs")

    project_id  = Variable.get("GCP_PROJECT_ID")
    credentials = _get_credentials()

    bq_client = bigquery.Client(credentials=credentials, project=project_id)
    table_id  = f"{project_id}.{BQ_DATASET}.{BQ_TABLE}"

    job_config = bigquery.LoadJobConfig(
        source_format     = bigquery.SourceFormat.PARQUET,
        write_disposition = bigquery.WriteDisposition.WRITE_APPEND,  # 날짜별 누적
        autodetect        = True,
    )

    load_job = bq_client.load_table_from_uri(
        gcs_path,
        table_id,
        job_config=job_config,
    )
    load_job.result()

    table = bq_client.get_table(table_id)
    logging.info(f"BigQuery 적재 완료: {table_id} (총 {table.num_rows}행)")


with DAG(
    dag_id="daily_to_gcs_bq_dag",
    description="일별 Parquet 변환 → GCS 업로드 → 로컬 삭제 → BigQuery 적재",
    start_date=datetime(2026, 4, 17),
    schedule_interval="0 1 * * *",
    catchup=False,
    tags=["air-quality", "daily", "gcs", "bigquery"],
) as dag:

    task_convert = PythonOperator(
        task_id="convert_to_parquet",
        python_callable=convert_to_parquet,
    )

    task_upload = PythonOperator(
        task_id="upload_to_gcs",
        python_callable=upload_to_gcs,
    )

    task_delete = PythonOperator(
        task_id="delete_local_files",
        python_callable=delete_local_files,
    )

    task_load = PythonOperator(
        task_id="load_to_bigquery",
        python_callable=load_to_bigquery,
    )

    task_convert >> task_upload >> task_delete >> task_load
