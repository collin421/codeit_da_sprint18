# Sprint 18: 에어코리아 대기오염 데이터 파이프라인

## 파일 구조

```
dags/sprint18/
├── air_quality_dag.py      # Airflow DAG (메인)
├── colab_test.py           # Google Colab 테스트 스크립트
└── bigquery_analysis.sql   # BigQuery 분석 쿼리 모음
```

## 실행 전 준비

### 1. Airflow Variables 등록
Airflow Web UI > Admin > Variables 에서 아래 두 변수를 등록합니다.

| Key | Value |
|-----|-------|
| `SERVICE_API_KEY` | 공공데이터포털 일반 인증키 (Decoding) |
| `GCP_SERVICE_ACCOUNT` | GCP 서비스 계정 JSON 문자열 전체 |

### 2. BigQuery 환경 설정
`air_quality_dag.py` 상단의 상수를 본인 환경에 맞게 수정하세요:

```python
GCP_PROJECT_ID = "본인의-GCP-프로젝트-ID"
BQ_DATASET     = "air_quality"
BQ_TABLE       = "realtime_measurements"
```

BigQuery 콘솔에서 데이터셋(`air_quality`)을 먼저 생성해두면,
테이블은 DAG 첫 실행 시 자동 생성됩니다.

### 3. 스케줄
- `schedule_interval="0 * * * *"` → 매 시간 정각 실행 (예: 1시, 2시, ...)
- `catchup=False` → 과거 누락 실행은 건너뜀

## DAG 흐름

```
fetch_air_quality_data  →  load_csv_to_bigquery
(API 호출 + CSV 저장)       (BigQuery WRITE_APPEND)
```

## 분석 쿼리 사용법
`bigquery_analysis.sql` 파일을 BigQuery 콘솔에 붙여넣고,
`본인의-GCP-프로젝트-ID` 를 실제 프로젝트 ID로 교체 후 실행하세요.
