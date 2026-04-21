# Sprint 18 미션 요약 및 제출 가이드

## 미션 개요
- 공공데이터포털의 한국환경공단 에어코리아 대기오염정보 데이터를 사용합니다.
- 특히 `시도별 실시간 측정정보 조회` API를 활용하여 데이터를 수집합니다.
- 목표: 데이터 엔지니어 시나리오로 Airflow DAG를 생성해 주기적인 수집 및 BigQuery 적재 구현.

## 수행 흐름
1. 오픈 API 승인 받기
   - 공공데이터포털 회원가입 및 로그인
   - 한국환경공단 에어코리아 대기오염정보 검색 및 활용신청
   - 시도별 실시간 측정정보 조회 체크 후 승인 완료
   - 마이페이지 > 데이터 활용 > Open API > 활용신청 현황 > 복사한 인증키(Decoding) 보관

2. API 정상 작동 확인
   - Colab/Jupyter Notebook에서 `requests`, `lxml` 설치 및 import
   - 받은 인증키, API 주소, request parameter 설정
   - `시도별 실시간 측정정보` 호출, 200 vs resultCode 00 확인

3. 데이터 확인 및 가공
   - XML 파싱 (`lxml.etree`)하여 item 요소 읽기
   - 예외 처리 함수 (`get_value` 등)로 '-' 혹은 빈 값 처리
   - pandas DataFrame으로 변환
   - 로컬 CSV로 저장

4. BigQuery 로드
   - `google-cloud-bigquery` 사용
   - GCP 서비스 계정 JSON 키 발급
   - BigQuery 데이터셋/테이블 준비 (`air_quality` 데이터셋 등)
   - `bq_client.load_table_from_file`를 통해 CSV 적재 (WRITE_APPEND)

5. Airflow DAG 구성
   - API 호출, 파싱, DataFrame 생성, CSV 저장, BigQuery 적재를 PythonOperator로 구현
   - DAG 스케줄링 설정 (예: 1시간마다 실행)
   - Airflow UI에서 Grid/Tree View로 실행 기록 확인

6. 결과 검증 및 분석
   - BigQuery에서 적재 결과 검증
   - 분석 예시: 시도별 PM2.5 평균, 오염물질 추이, 상관관계 분석 등
   - 제출: Airflow DAG 파일 + Bar chart 캡쳐 + 쿼리 결과물

## 데이터 항목 정의 (API 응답)
- resultCode, resultMsg, numOfRows, pageNo, totalCount, items
- stationName, mangName, dataTime
- so2Value, coValue, o3Value, no2Value, pm10Value, pm10Value24, pm25Value, pm25Value24
- khaiValue, khaiGrade, so2Grade, coGrade, o3Grade, no2Grade, pm10Grade, pm25Grade
- pm10Grade1h, pm25Grade1h, so2Flag, coFlag, o3Flag, no2Flag, pm10Flag, pm25Flag

## 제출 구성
- Airflow DAG Python 파일 (소스코드)
- Google Colab 실행 기록 / 스크린샷
- `pm25_bar_chart.png` 등의 시각화 결과
- BigQuery 쿼리 및 분석 결과 요약

## 참고 사항
- API Key 및 서비스 계정 키는 민감정보이므로 공개 저장소에 업로드 금지
- 스케줄링 동작 확인은 Airflow UI에서 꼭 수행
- `df_all.to_csv("air_quality_latest.csv", index=False, encoding="utf-8-sig")` 등의 한글 인코딩 유의
