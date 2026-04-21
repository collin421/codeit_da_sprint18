<div align="center">

# 에어코리아 대기오염 데이터 파이프라인

**공공데이터포털 API → Airflow → BigQuery 로 이어지는 시간별 자동 수집·분석 파이프라인**

</div>

---

## 요약

한국환경공단 에어코리아 OpenAPI 에서 **전국 약 700개 측정소**의 실시간 대기질 데이터를 매시간 자동 수집 → BigQuery 에 누적 적재 → Jupyter 노트북에서 SQL 로 집계·시각화.

**분석 산출물 (Jupyter 노트북)**
- 시도별 PM2.5 평균 (막대그래프)
- 시간대별 PM10/PM2.5 추이 (선그래프)
- 오염물질 간 상관관계 (히트맵)

---

## 기술 스택

| 단계 | 도구 |
|---|---|
| 오케스트레이션 | Apache Airflow (Docker Compose · LocalExecutor) |
| 데이터 수집 | `requests` + `pd.read_xml()` |
| 중간 저장 | 로컬 CSV (`utf-8-sig`) |
| 데이터 웨어하우스 | Google BigQuery |
| 분석·시각화 | pandas + matplotlib + koreanize-matplotlib |

---

## 프로젝트 구조

```
코드잇12_스프린트18/
├── dags/
│   └── air_quality_dag_bq.py            # Airflow DAG (Task 2개: 수집 + 적재)
├── sprint18/
│   ├── sprint18_colab_hardcoding.ipynb  # Colab/Jupyter 수집 실습 노트북
│   ├── sprint18_analysis.ipynb          # BigQuery 분석 노트북 (차트 3종)
│   ├── bigquery_analysis.sql            # BQ 분석 쿼리
│   ├── service_account_key.json         # GCP 인증키 (커밋 금지)
│   └── sprint18_data.csv                # 샘플 데이터
├── config/                               # Airflow 설정
├── plugins/                              # Airflow 플러그인
├── Dockerfile
├── docker-compose.yaml
├── requirements.txt
└── .env                                  # 환경변수 (커밋 금지)
```

---

## 실행 순서

### 1. 공공데이터포털 API 신청 및 키 발급

- [공공데이터포털](https://www.data.go.kr/) 회원가입·로그인
- **한국환경공단 에어코리아 대기오염정보** 검색 → 활용신청
- 마이페이지 → 데이터활용 → Open API → 활용신청 현황 → **일반 인증키(Decoding) 복사**

### 2. API 엔드포인트 확인 — 시도별 실시간 측정정보 조회

- **요청 주소**: `http://apis.data.go.kr/B552584/ArpltnInforInqireSvc/getCtprvnRltmMesureDnsty`
- 파이썬 샘플 코드로 호출 구조 확인 (구성 파악용)

### 3. API 파라미터 확인 (기술문서)

| 항목 | 값 | 설명 |
|---|---|---|
| `serviceKey` | 발급받은 인증키 | 필수 |
| `sidoName` | **전국** | 17개 시도를 **한 번의 호출**로 수신 |
| `ver` | `1.3` | PM10·PM2.5 1시간 등급 자료 포함 |
| `returnType` | `xml` | 응답 형식 |
| `numOfRows` | `1000` | 측정소 약 700개 여유있게 커버 |

### 4. GCP 설정

- GCP 프로젝트에서 **서비스 계정** 생성
- **BigQuery 관리자** 권한 부여
- **JSON 키** 생성 → 다운로드

### 5. 의존성 추가 (Dockerfile / requirements.txt)

`requirements.txt` 에 필요한 라이브러리 추가:

```
apache-airflow-providers-google
google-cloud-bigquery
pandas
requests
lxml
```

### 6. Airflow 실행 (Docker Compose)

```bash
docker compose up -d
```

컨테이너 기동 후 `localhost:8081` 로 접속 (아이디/비번: `airflow` / `airflow`).

### 7. Airflow Variables 등록

Airflow Web UI → **Admin → Variables** 에서 2개 등록:

| Key | Value |
|---|---|
| `SERVICE_API_KEY` | 1단계에서 복사한 공공데이터포털 Decoding 인증키 |
| `GCP_SERVICE_ACCOUNT` | 4단계에서 다운로드한 서비스 계정 JSON 파일 **전체 내용**을 한 줄 문자열로 |

**JSON → 한 줄 문자열 변환** (PowerShell 또는 Git Bash)

```bash
cat service_account_key.json | python -c "import sys,json; print(json.dumps(json.load(sys.stdin)))"
```

출력된 한 줄 결과를 Variable 값에 그대로 붙여넣기.

### 8. DAG 실행

Airflow UI 에서 `sprint18_air_quality_bq` DAG 활성화.
매시간 정각 자동 실행. 수동 실행은 **Trigger DAG** 버튼.

---

## DAG 코드

`dags/air_quality_dag_bq.py`

```python
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
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "your-gcp-project-id")
BQ_DATASET     = os.environ.get("BQ_DATASET_ID",  "codeit")
BQ_TABLE       = os.environ.get("BQ_TABLE_NAME",  "sprint18_air_quality")

# 에어코리아 API 주소
API_URL = "http://apis.data.go.kr/B552584/ArpltnInforInqireSvc/getCtprvnRltmMesureDnsty"

# CSV 저장 폴더 — 파일명은 실행 시각으로 동적 생성
DATA_DIR = "/opt/airflow/dags/data"


# Task 1: API 호출 → CSV 저장
def fetch_air_quality_data():

    api_key = Variable.get("SERVICE_API_KEY")

    params = {
        "serviceKey": api_key,
        "returnType": "xml",
        "numOfRows":  "1000",   # 전국 측정소 약 700개, 여유있게 설정
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

    # 시간별 CSV 파일 경로 생성 — 예: data/data_2026-04-21_15.csv
    os.makedirs(DATA_DIR, exist_ok=True)
    csv_path = f"{DATA_DIR}/data_{now_kst.strftime('%Y-%m-%d_%H')}.csv"

    # CSV 저장, 엑셀에서 확인 위해 utf-8-sig 로 인코딩
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")


# Task 2: CSV → BigQuery 적재
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

    # Task 1 에서 저장한 CSV 경로 (같은 시각 기준 동일 파일)
    k_time = timezone(timedelta(hours=9))
    csv_path = f"{DATA_DIR}/data_{datetime.now(k_time).strftime('%Y-%m-%d_%H')}.csv"

    # 파일 열어서 BigQuery 에 업로드
    with open(csv_path, "rb") as csv_file:
        load_job = bq_client.load_table_from_file(csv_file, table_id, job_config=job_config)
        load_job.result()   # 업로드 완료까지 대기


# DAG 정의
with DAG(
    dag_id="sprint18_air_quality_bq",
    description="API 호출 → 로컬 CSV → BigQuery 적재",
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 * * * *",   # 매시간 0분 실행
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
```

---

## 분석 노트북 (`sprint18/`)

### `sprint18_colab_hardcoding.ipynb` — 수집 실습
DAG 없이도 API 호출 → XML 파싱 → CSV 저장 → BigQuery 적재까지 **셀 단위로 실습** 가능.

### `sprint18_analysis.ipynb` — 분석
BigQuery 에서 SQL 로 집계해 차트 3장 생성.

| 분석 | 핵심 SQL | 산출물 |
|---|---|---|
| 시도별 PM2.5 평균 (24h) | `GROUP BY sidoName` + `AVG` | `pm25_bar_chart.png` |
| 시간대별 PM10/PM2.5 추이 (7일) | `TIMESTAMP_TRUNC(..., HOUR)` + `AVG` | `trend_chart.png` |
| 오염물질 상관관계 (7일) | `SAFE_CAST` + pandas `.corr()` | `correlation_heatmap.png` |

---

## 분석 결과 & 인사이트

> 데이터 기간: **2026-04-17 ~ 2026-04-21** (약 4일, 52,416행, 672개 측정소)

### 1. 시도별 PM2.5 평균 (최근 24시간)

![PM2.5 Bar Chart](sprint18/pm25_bar_chart.png)

**핵심 인사이트**
- 17개 시도 모두 "보통" 등급 구간(15~35 μg/m³) 내 → 이 시점 경보 수준 지역 없음
- **최고**: 광주 25.5 · 전북 25.3 (서남권 평균 ↑)
- **최저**: 경남 14.3 · 부산 15.7 (동남권 해안지역 양호)
- 수도권(서울 21.2 / 경기 21.1 / 인천 21.7)은 전국 평균 수준

### 2. PM10 / PM2.5 시간대별 추이 (최근 7일)

![Trend Chart](sprint18/trend_chart.png)

**핵심 인사이트**
- **04-20 오후부터 PM10 이 40 → 138 μg/m³ 로 3배 이상 급상승** (7일 기간 중 최고점)
- 급격한 상승 패턴 = **황사 / 외부 유입**의 전형적 특징
- 반면 **PM2.5 는 10~22 범위에서 안정** → 굵은 입자(PM10) 위주로 증가
- 즉, 이 시점 대기질 악화는 **국내 배출원보다 외부 유입 요인**

### 3. 오염물질 간 상관관계 (최근 7일)

![Correlation Heatmap](sprint18/correlation_heatmap.png)

**핵심 인사이트**
- **PM10 ↔ PM2.5 = 0.48** : 미세먼지끼리 **출처가 유사**해서 동반 상승
- **CO ↔ NO2 = 0.39** : 자동차 배기·화력발전 같은 **연소 기반 오염원 공통성**
- **O3 ↔ NO2 = −0.36** (음상관) : 햇빛 → NO2가 O3로 변환되는 **광화학 반응**. 한쪽이 오르면 반대쪽이 감소
- SO2, CO 와 미세먼지 상관은 약함(0.01~0.16) → **미세먼지 발생 경로가 가스성 오염과 독립적**

### 종합 요약

| 질문 | 결론 |
|---|---|
| 어느 지역이 나쁜가? | **광주·전북** (서남권) |
| 어느 지역이 좋은가? | **경남·부산** (동남권) |
| 최근 7일 가장 큰 이슈? | **04-20 오후 PM10 급등** (황사 추정) |
| 미세먼지는 단독 현상인가? | 아님 — **미세먼지끼리 동반** 하지만 가스(SO2/CO)와는 별개 |
| O3(오존)은 언제 높아지나? | **NO2가 낮을 때** (광화학 반응으로 반비례) |

---

> ## **유의 사항 — 보안**
>
> ---
>
> **API 키 · 서비스 계정 JSON 은 절대 공개 저장소에 업로드 금지.**
>
> 한 번 커밋되면 Git 이력에서 완전 제거가 어려워 키 유출 위험이 큼. 사전에 `.gitignore` 로 차단 필수.
>
> ---
>
> **`.gitignore` 권장 내용**
>
> ```gitignore
> # GCP 인증 키
> *service_account*.json
> project-*.json
>
> # 환경변수
> .env
>
> # Airflow 런타임
> logs/
> dags/data/
> ```
>
> ---
>
> **커밋 전 체크**
>
> - `git status` 실행 시 `service_account_key.json`, `project-*.json`, `.env` 가 **Untracked** 목록에도 안 보여야 정상
> - 혹시 이미 커밋됐다면 `git rm --cached <파일>` 로 인덱스에서 제거 후 다시 커밋, 그리고 **해당 키는 즉시 폐기·재발급**
> - DAG 코드에 실제 GCP 프로젝트 ID 가 하드코딩되지 않았는지 확인 (플레이스홀더 `your-gcp-project-id` 유지 또는 `.env` 로 분리)

---

## 참고 문서

- [공공데이터포털](https://www.data.go.kr/)
- [한국환경공단 에어코리아 OpenAPI](https://www.data.go.kr/data/15073861/openapi.do)
- [Google Cloud BigQuery 문서](https://cloud.google.com/bigquery/docs)
- [Apache Airflow 공식 문서](https://airflow.apache.org/docs/)

---

<div align="center">

**코드잇 데이터 엔지니어링 스프린트 18**
_한국환경공단 에어코리아 OpenAPI 기반_

</div>
