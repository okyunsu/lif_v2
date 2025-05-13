-- 1. 모든 회사 목록과 각 회사의 재무제표 데이터 수 조회
SELECT 
    c.corp_name AS 회사명, 
    c.corp_code AS 회사코드,
    c.stock_code AS 주식코드,
    COUNT(DISTINCT f.bsns_year) AS 보유연도수,
    STRING_AGG(DISTINCT f.bsns_year, ', ' ORDER BY f.bsns_year DESC) AS 보유연도목록,
    COUNT(f.id) AS 재무데이터수
FROM 
    companies c
LEFT JOIN 
    financials f ON c.corp_code = f.corp_code
GROUP BY 
    c.corp_code, c.corp_name, c.stock_code
ORDER BY 
    COUNT(f.id) DESC;

-- 2. 특정 회사의 재무제표 유형별 데이터 수 조회
-- '삼성전자'를 원하는 회사명으로 변경하세요
SELECT 
    c.corp_name AS 회사명,
    f.bsns_year AS 사업연도,
    s.sj_nm AS 재무제표유형,
    COUNT(f.id) AS 항목수
FROM 
    companies c
JOIN 
    financials f ON c.corp_code = f.corp_code
JOIN 
    statement s ON f.sj_div = s.sj_div
WHERE 
    c.corp_name = '삼성전자'
GROUP BY 
    c.corp_name, f.bsns_year, s.sj_nm
ORDER BY 
    f.bsns_year DESC, s.sj_nm;

-- 3. 특정 회사의 주요 재무 지표 조회
-- '삼성전자'를 원하는 회사명으로 변경하세요
SELECT 
    c.corp_name AS 회사명,
    f.bsns_year AS 사업연도,
    s.sj_nm AS 재무제표유형,
    f.account_nm AS 계정명,
    f.thstrm_nm AS 당기명,
    f.thstrm_amount AS 당기금액,
    f.frmtrm_nm AS 전기명,
    f.frmtrm_amount AS 전기금액
FROM 
    companies c
JOIN 
    financials f ON c.corp_code = f.corp_code
JOIN 
    statement s ON f.sj_div = s.sj_div
WHERE 
    c.corp_name = '삼성전자'
    AND f.account_nm IN ('자산총계', '부채총계', '자본총계', '매출액', '영업이익', '당기순이익')
ORDER BY 
    f.bsns_year DESC, s.sj_nm, f.ord;

-- 4. 재무제표가 없는 회사 목록 조회
SELECT 
    c.corp_name AS 회사명,
    c.corp_code AS 회사코드,
    c.stock_code AS 주식코드
FROM 
    companies c
LEFT JOIN 
    financials f ON c.corp_code = f.corp_code
WHERE 
    f.id IS NULL
ORDER BY 
    c.corp_name;

-- 5. 가장 최근 연도의 재무제표 데이터가 있는 회사 목록
WITH latest_years AS (
    SELECT 
        corp_code, 
        MAX(bsns_year) AS latest_year
    FROM 
        financials
    GROUP BY 
        corp_code
)
SELECT 
    c.corp_name AS 회사명,
    ly.latest_year AS 최근연도,
    COUNT(f.id) AS 데이터수
FROM 
    companies c
JOIN 
    latest_years ly ON c.corp_code = ly.corp_code
JOIN 
    financials f ON c.corp_code = f.corp_code AND f.bsns_year = ly.latest_year
GROUP BY 
    c.corp_name, ly.latest_year
ORDER BY 
    ly.latest_year DESC, c.corp_name;

-- 6. 특정 회사의 모든 재무 데이터 상세 조회
-- '삼성전자'를 원하는 회사명으로 변경하세요
SELECT 
    c.corp_name AS 회사명,
    f.bsns_year AS 사업연도,
    s.sj_nm AS 재무제표유형,
    f.account_nm AS 계정명,
    f.thstrm_nm AS 당기명,
    f.thstrm_amount AS 당기금액,
    f.frmtrm_nm AS 전기명,
    f.frmtrm_amount AS 전기금액,
    f.bfefrmtrm_nm AS 전전기명,
    f.bfefrmtrm_amount AS 전전기금액
FROM 
    companies c
JOIN 
    financials f ON c.corp_code = f.corp_code
JOIN 
    statement s ON f.sj_div = s.sj_div
WHERE 
    c.corp_name = '삼성전자'
    AND f.bsns_year = (SELECT MAX(bsns_year) FROM financials WHERE corp_code = c.corp_code)
ORDER BY 
    s.sj_nm, f.ord; 