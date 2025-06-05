"""
FastAPI 앱의 진입점.
라우터들을 등록하고 서버를 실행합니다.
"""

import uvicorn
from fastapi import FastAPI
from app.api import cafe_search, cafe_detail, keyword_extract

app = FastAPI(
    title="카페 감수광 크롤링 API",
    description="카카오맵 기반의 카페 검색/상세/키워드 분석 API",
    version="1.0.0"
)

# 라우터 등록
app.include_router(cafe_search.router, prefix="/api/v1/cafe", tags=["Cafe Search"])
app.include_router(cafe_detail.router, prefix="/api/v1/cafe", tags=["Cafe Detail"])
app.include_router(keyword_extract.router, prefix="/api/v1/keywords", tags=["Keyword Extract"])

if __name__ == "__main__":
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)