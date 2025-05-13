# 금융 데이터 마이크로서비스

이 프로젝트는 금융 데이터 처리 및 분석을 위한 마이크로서비스 아키텍처를 구현합니다. 시스템은 세 가지 주요 서비스로 구성됩니다:

- **financeservice**: DART API에서 금융 데이터 크롤링 담당
- **ratioservice**: 재무 비율 계산 수행
- **gateway**: 적절한 서비스로 요청을 라우팅하는 API 게이트웨이

## 아키텍처 개요

```
┌─────────┐      ┌──────────────┐      ┌───────────────┐
│ 클라이언트 │─────▶│   게이트웨이   │─────▶│ financeservice│
└─────────┘      └──────────────┘      └───────────────┘
                        │
                        │              ┌───────────────┐
                        └─────────────▶│  ratioservice │
                                       └───────────────┘
```

두 서비스는 데이터 저장 및 검색을 위해 동일한 PostgreSQL 데이터베이스를 공유합니다.

## 설치 안내

### 사전 요구사항

- Docker 및 Docker Compose
- PostgreSQL 클라이언트 (수동 테이블 생성용)

### 빌드 및 실행

1. 저장소 복제
2. Docker 이미지 빌드:

```bash
docker-compose build
```

3. **중요**: 서비스 시작:

```bash
docker-compose up -d
```

4. **중요**: financeservice 컨테이너 중지:

```bash
docker-compose stop financeservice
```

5. 필요한 데이터베이스 테이블 생성 (`db` 디렉토리의 SQL 스크립트 참조):

```bash
# PostgreSQL 데이터베이스에 연결
psql -h localhost -p 5432 -U [사용자명] -d [데이터베이스명]

# SQL 스크립트를 실행하여 테이블 생성
# 예: \i db/create_tables.sql
```

6. financeservice 재시작:

```bash
docker-compose start financeservice
```

> **참고**: financeservice는 시작하기 전에 데이터베이스 테이블이 생성되어 있어야 합니다. 이 수동 단계는 임시적이며 향후 업데이트에서 자동화될 예정입니다.

## API 엔드포인트

### 게이트웨이

- 기본 URL: `http://localhost:8000/api`

### Finance Service

- 회사 정보 조회: `GET /finance/company/{company_name}`
- 재무제표 조회: `GET /finance/statements/{company_name}`

### Ratio Service

- 재무 비율 조회: `GET /ratio/financial-ratios/{company_name}`
- 성장률 조회: `GET /ratio/growth-rates/{company_name}`

## 알려진 이슈

- financeservice를 시작하기 전에 데이터베이스 테이블을 수동으로 생성해야 함 (향후 업데이트에서 수정 예정)
- 각 서비스에 대한 추가 문서가 필요함

## 향후 개선 사항

- 데이터베이스 초기화 자동화
- 서비스 상태 모니터링
- 인증 및 권한 부여
- 종합적인 API 문서
- CI/CD 파이프라인 설정 