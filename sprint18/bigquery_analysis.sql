-- ============================================================
-- Sprint 18: BigQuery 대기오염 분석 쿼리 모음
-- 테이블: `project-87361850-33e3-4f44-b0f.codeit.sprint18_air_quality`
-- ============================================================

-- 아래 SET 구문으로 테이블 경로를 한 번만 수정하세요.
-- (BigQuery는 변수를 지원하지 않으므로, 아래 쿼리에서
--  직접 테이블 경로를 교체하여 실행하세요)

-- ──────────────────────────────────────────────
-- 쿼리 1. 최신 수집 데이터 미리보기
-- ──────────────────────────────────────────────
SELECT *
FROM `project-87361850-33e3-4f44-b0f.codeit.sprint18_air_quality`
ORDER BY collected_at DESC
LIMIT 20;


-- ──────────────────────────────────────────────
-- 쿼리 2. 시도별 평균 대기오염 농도 비교
--         (현재 시간 기준 가장 최근 데이터)
-- ──────────────────────────────────────────────
SELECT
    sidoName                        AS 시도,
    ROUND(AVG(SAFE_CAST(pm10Value  AS FLOAT64)), 2) AS 평균_PM10,
    ROUND(AVG(SAFE_CAST(pm25Value  AS FLOAT64)), 2) AS 평균_PM25,
    ROUND(AVG(SAFE_CAST(so2Value   AS FLOAT64)), 4) AS 평균_SO2,
    ROUND(AVG(SAFE_CAST(no2Value   AS FLOAT64)), 4) AS 평균_NO2,
    ROUND(AVG(SAFE_CAST(o3Value    AS FLOAT64)), 4) AS 평균_O3,
    ROUND(AVG(SAFE_CAST(coValue    AS FLOAT64)), 3) AS 평균_CO,
    ROUND(AVG(SAFE_CAST(khaiValue  AS FLOAT64)), 1) AS 평균_통합대기환경수치,
    COUNT(*)                        AS 측정소_수
FROM `project-87361850-33e3-4f44-b0f.codeit.sprint18_air_quality`
WHERE collected_at = (
    SELECT MAX(collected_at)
    FROM `project-87361850-33e3-4f44-b0f.codeit.sprint18_air_quality`
)
GROUP BY sidoName
ORDER BY 평균_PM25 DESC;


-- ──────────────────────────────────────────────
-- 쿼리 3. 서울 시간대별 PM2.5 추세 (수집 시간 기준)
-- ──────────────────────────────────────────────
SELECT
    collected_at                                        AS 수집시각,
    ROUND(AVG(SAFE_CAST(pm25Value AS FLOAT64)), 2)     AS 평균_PM25,
    ROUND(AVG(SAFE_CAST(pm10Value AS FLOAT64)), 2)     AS 평균_PM10,
    ROUND(AVG(SAFE_CAST(khaiValue AS FLOAT64)), 1)     AS 평균_통합대기환경수치
FROM `project-87361850-33e3-4f44-b0f.codeit.sprint18_air_quality`
WHERE sidoName = '서울'
GROUP BY collected_at
ORDER BY collected_at;


-- ──────────────────────────────────────────────
-- 쿼리 4. 측정소별 오염물질 최고 농도 TOP 10 (PM2.5 기준)
-- ──────────────────────────────────────────────
SELECT
    sidoName                                            AS 시도,
    stationName                                         AS 측정소,
    MAX(SAFE_CAST(pm25Value AS FLOAT64))               AS 최대_PM25,
    MAX(SAFE_CAST(pm10Value AS FLOAT64))               AS 최대_PM10,
    MAX(SAFE_CAST(no2Value  AS FLOAT64))               AS 최대_NO2,
    MAX(SAFE_CAST(o3Value   AS FLOAT64))               AS 최대_O3
FROM `project-87361850-33e3-4f44-b0f.codeit.sprint18_air_quality`
GROUP BY sidoName, stationName
ORDER BY 최대_PM25 DESC NULLS LAST
LIMIT 10;


-- ──────────────────────────────────────────────
-- 쿼리 5. 통합대기환경지수별 측정소 분포
--         (khaiGrade: 1=좋음, 2=보통, 3=나쁨, 4=매우나쁨)
-- ──────────────────────────────────────────────
SELECT
    sidoName                    AS 시도,
    khaiGrade                   AS 통합대기환경지수,
    CASE khaiGrade
        WHEN '1' THEN '좋음'
        WHEN '2' THEN '보통'
        WHEN '3' THEN '나쁨'
        WHEN '4' THEN '매우나쁨'
        ELSE '미측정'
    END                         AS 등급명,
    COUNT(*)                    AS 측정소_수
FROM `project-87361850-33e3-4f44-b0f.codeit.sprint18_air_quality`
WHERE collected_at = (
    SELECT MAX(collected_at)
    FROM `project-87361850-33e3-4f44-b0f.codeit.sprint18_air_quality`
)
GROUP BY sidoName, khaiGrade
ORDER BY sidoName, khaiGrade;


-- ──────────────────────────────────────────────
-- 쿼리 6. 오염물질 간 상관관계 확인용 (서울, 최근 24회 수집)
-- ──────────────────────────────────────────────
SELECT
    collected_at,
    ROUND(AVG(SAFE_CAST(pm25Value AS FLOAT64)), 2) AS pm25,
    ROUND(AVG(SAFE_CAST(pm10Value AS FLOAT64)), 2) AS pm10,
    ROUND(AVG(SAFE_CAST(no2Value  AS FLOAT64)), 4) AS no2,
    ROUND(AVG(SAFE_CAST(o3Value   AS FLOAT64)), 4) AS o3,
    ROUND(AVG(SAFE_CAST(so2Value  AS FLOAT64)), 4) AS so2,
    ROUND(AVG(SAFE_CAST(coValue   AS FLOAT64)), 3) AS co,
    ROUND(AVG(SAFE_CAST(khaiValue AS FLOAT64)), 1) AS khai
FROM `project-87361850-33e3-4f44-b0f.codeit.sprint18_air_quality`
WHERE sidoName = '서울'
GROUP BY collected_at
ORDER BY collected_at DESC
LIMIT 24;
