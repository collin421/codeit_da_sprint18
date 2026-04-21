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

# CSV 저장 폴더
DATA_DIR = "/opt/airflow/dags/data"


# Task 1: API 호출 → 일별 CSV 에 append

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

    # pd.read_xml 으로 바로 DataFrame 변환
    df = pd.read_xml(response.content, xpath="//item")
    print(f"전국 측정소 수: {len(df)}건")

    # 수집 시각 컬럼 추가 — 9시간 추가하여 한국 시간대로 맞춤
    kst = timezone(timedelta(hours=9))
    now_kst = datetime.now(kst)
    df["collected_at"] = now_kst.strftime("%Y-%m-%d %H:%M:%S")

    os.makedirs(DATA_DIR, exist_ok=True)

    # 일별 CSV — 하루 동안 같은 파일에 시간별로 행만 추가됨
    # 예: data_2026-04-21.csv 에 10시/11시/12시... 데이터가 차곡차곡 누적
    daily_csv = f"{DATA_DIR}/data_{now_kst.strftime('%Y-%m-%d')}.csv"

    # 파일이 없으면 헤더 포함해서 처음부터 생성, 있으면 append 모드로 행만 추가
    if os.path.exists(daily_csv):
        df.to_csv(daily_csv, mode="a", header=False, index=False, encoding="utf-8-sig")
    else:
        df.to_csv(daily_csv, mode="w", header=True,  index=False, encoding="utf-8-sig")

    # BigQuery 업로드용 "최신 시간 스냅샷" — 매시간 덮어쓰기
    # (일별 CSV 를 그대로 올리면 중복 적재되므로, 이번 시간 데이터만 담긴 별도 파일 사용)
    latest_csv = f"{DATA_DIR}/_latest.csv"
    df.to_csv(latest_csv, index=False, encoding="utf-8-sig")


# Task 2: 최신 시간 CSV → BigQuery 적재

def load_csv_to_bigquery():

    # 서비스 계정 정보 가져오기
    json_string  = Variable.get("GCP_SERVICE_ACCOUNT")
    service_info = json.loads(json_string)

    # 서비스 계정으로 인증 후 BigQuery 클라이언트 객체 생성
    credentials = service_account.Credentials.from_service_account_info(service_info)
    bq_client   = bigquery.Client(credentials=credentials, project=GCP_PROJECT_ID)

    # 적재할 테이블 경로: 프로젝트.데이터셋.테이블
    table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"

    # 적재 설정
    job_config = bigquery.LoadJobConfig(
        autodetect=True,                                           # 컬럼 타입 자동 감지
        source_format=bigquery.SourceFormat.CSV,                   # CSV 형식
        skip_leading_rows=1,                                       # 헤더 행 건너뜀
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,  # 기존 데이터 유지하고 추가
    )

    # 이번 시간 데이터만 담긴 스냅샷 파일 (Task 1 에서 매번 덮어씀)
    csv_path = f"{DATA_DIR}/_latest.csv"

    # 파일 열어서 BigQuery 에 업로드
    with open(csv_path, "rb") as csv_file:
        load_job = bq_client.load_table_from_file(csv_file, table_id, job_config=job_config)
        load_job.result()  # 업로드 완료까지 대기


# DAG 정의

with DAG(
    dag_id="sprint18_air_quality_bq",
    description="API 호출 → 일별 CSV append → BigQuery 적재 (매시 20분)",
    start_date=datetime(2025, 1, 1),
    # 매시 20분 실행 — 기술문서상 "매시 15분 내외" 데이터 생성 후 여유 5분
    schedule_interval="20 * * * *",
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
