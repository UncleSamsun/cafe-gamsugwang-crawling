# 카페 감수광 크롤링 프로젝트

제주 지역 카페에 대한 리뷰 데이터를 수집하고, 키워드 추출 및 클러스터링을 통해 사용자 맞춤 추천에 활용 가능한 인프라를 구축한 프로젝트입니다.

---

## 주요 기능
- 카카오맵 기반 카페 검색 및 크롤링 자동화
- 카페 리뷰 수집 및 저장
- 형태소 분석기(Kiwi) 기반 키워드 추출
- KR-SBERT 임베딩 기반 의미 유사 키워드 클러스터링
- 클러스터별 대표 키워드 선정 (TF-IDF + 거리 기반)
- 클러스터링 결과 및 키워드 DB 저장

---

## 🛠 사용 기술 스택

### 백엔드
- Python 3.11
- FastAPI: 비동기 API 서버 프레임워크
- MySQL + PyMySQL: 리뷰 및 키워드 저장
- Redis: 작업 상태 캐싱
- kiwipiepy: 형태소 분석기
- scikit-learn, HDBSCAN: 키워드 클러스터링
- sentence-transformers (KR-SBERT): 임베딩 모델

### 인프라 및 테스트
- Docker / Docker Compose: 개발 환경 구성
- pytest: 유닛 테스트

---

## 📁 디렉터리 구조

```
cafe-gamsugwang-crawling/
├── app/
│   ├── api/                    # FastAPI 라우터
│   ├── core/                   # DB, Redis 클라이언트
│   ├── geo/                    # 위치 유틸
│   └── service/                # 비즈니스 로직
├── data/
│   ├── geo/                    # GeoJSON 및 위치 정보
│   └── map/                    # 시각화용 HTML
├── sql/                        # 테이블 초기화 SQL
├── tests/                      # 유닛 테스트 코드
├── .env
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── main.py
```

---

## 🧾 데이터베이스 구조

| 테이블명            | 설명                          |
|---------------------|-------------------------------|
| cafes               | 카페 기본 정보               |
| menus               | 카페별 대표 메뉴             |
| kakao_reviews       | 크롤링된 리뷰                |
| extracted_keywords  | 리뷰에서 추출된 키워드       |
| clustered_keywords  | 클러스터 내 키워드 + count   |
| keywords            | 클러스터 대표 키워드 + 총 count (기존 cluster_summaries) |

---

## ⚙️ API 라우터 목록

| 설명                          | 메서드 | 엔드포인트                       |
|-------------------------------|--------|----------------------------------|
| 카페ID 조회                   | POST   | /api/v1/cafe/search              |
| 카페 데이터 크롤링           | POST   | /api/v1/cafe/detail              |
| 키워드 분석 실행             | POST   | /api/v1/keywords                 |
| 크롤링 상태 조회             | GET    | /api/v1/cafe/search/{jobId}     |
| 최근 카페 크롤링 결과 조회  | GET    | /api/v1/cafe/detail/{jobId}     |
| 키워드 분석 결과 조회       | GET    | /api/v1/keywords/{jobId}        |

---

## 🚀 개선 예정 사항

- OpenAI API 등을 활용한 고품질 키워드 필터링 및 자동화
- 크롤링 구조 개선: 병렬 처리 최적화 및 성능 개선
- API 중복 요청 방지를 위한 락 처리 혹은 idempotent 설계 도입

---

## 📝 실행 방법

```bash
# 패키지 설치
pip install -r requirements.txt

# FastAPI 실행
uvicorn app.main:app --reload

# 도커 실행
docker-compose up -d
```

---

문의 사항은 댓글 또는 이슈로 등록해주세요 😊