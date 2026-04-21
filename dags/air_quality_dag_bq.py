import json
import os
from datetime import datetime, timezone, timedelta

import pandas as pd
import requests

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from google.cloud import bigquery
from google.oauth2 import service_account

# GCP / BigQuery 설정 (.env 에서 읽어옴, 없으면 기본값 사용)
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "project-87361850-33e3-4f44-b0f")
BQ_DATASET     = os.environ.get("BQ_DATASET_ID",  "codeit")
BQ_TABLE       = os.environ.get("BQ_TABLE_NAME",  "sprint18_air_quality")

# 에어코리아 API 주소
API_URL = "http://apis.data.go.kr/B552584/ArpltnInforInqireSvc/getCtprvnRltmMesureDnsty"

# CSV 저장 폴더 (파일명은 실행 시각으로 동적 생성)
DATA_DIR = "/opt/airflow/dags/data"


# Task 1: API 호출 → CSV 저장

def fetch_air_quality_data():

    api_key = Variable.get("SERVICE_API_KEY")

    params = {
        "serviceKey": api_key,
        "returnType": "xml",
        "numOfRows":  "1000",  # 전국 측정소 약 700개, 여유있게 설정
        "pageNo":     "1",
        "sidoName":   "전국",
        "ver":        "1.3",
    }

    # API 호출
    response = requests.get(API_URL, params=params, timeout=30)
    print(f"전국 호출 완료: status={response.status_code}")

    # pd.read_xml 으로 item 바로 DataFrame 변환
    df = pd.read_xml(response.content, xpath="//item")
    print(f"전국 측정소 수: {len(df)}건")

    # 수집 시각 컬럼 추가 (KST = UTC+9)
    kst = timezone(timedelta(hours=9))
    now_kst = datetime.now(kst)
    df["collected_at"] = now_kst.strftime("%Y-%m-%d %H:%M:%S")

    # 시간별 CSV 파일 경로 생성 (예: data/air_quality_2026-03-18_15.csv)
    os.makedirs(DATA_DIR, exist_ok=True)
    csv_path = f"{DATA_DIR}/data_{now_kst.strftime('%Y-%m-%d_%H')}.csv"

    # CSV 저장
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")


# Task 2: CSV → BigQuery 적재

def load_csv_to_bigquery():

    # 서비스 계정 정보 가져오기
    json_string  = Variable.get("GCP_SERVICE_ACCOUNT")
    service_info = json.loads(json_string)

    # 서비스 계정으로 인증 후 빅쿼리 클라이언트 객체 생성
    credentials = service_account.Credentials.from_service_account_info(service_info)
    bq_client   = bigquery.Client(credentials=credentials, project=GCP_PROJECT_ID)

    # 적재할 테이블 경로, 프로젝트.데이터셋.테이블
    table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"

    # 적재 설정
    job_config = bigquery.LoadJobConfig(
        autodetect=True,                                           # 컬럼 타입 자동 감지
        source_format=bigquery.SourceFormat.CSV,                   # CSV 형식
        skip_leading_rows=1,                                       # 헤더 행 건너뜀
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,  # 기존 데이터 유지하고 추가
    )

    # Task 1 에서 저장한 CSV 경로 (같은 시각 기준)
    k_time = timezone(timedelta(hours=9))
    csv_path = f"{DATA_DIR}/data_{datetime.now(k_time).strftime('%Y-%m-%d_%H')}.csv"

    # 파일 열어서 빅쿼리에 업로드
    with open(csv_path, "rb") as csv_file:
        load_job = bq_client.load_table_from_file(csv_file, table_id, job_config=job_config)
        load_job.result()  # 업로드 완료까지 대기


# DAG 정의

with DAG(
    dag_id="sprint18_air_quality_bq",
    description="API호출하여 로컬에 적재하고 빅쿼리 전송",
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 * * * *", # 매시간 0분에 실행
    catchup=False,
) as dag:

    task_fetch = PythonOperator(
        task_id="fetch_air_quality_data",
        python_callable=fetch_air_quality_data,
    )

    task_load = PythonOperator(
        task_id="load_csv_to_bigquery",
        python_callable=load_csv_to_bigquery,
    )

    task_fetch >> task_load
