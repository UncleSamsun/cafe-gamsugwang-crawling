from fastapi import APIRouter
from app.service.cafe_search import run_grid_crawling

router = APIRouter()

@router.get(
    "/",
    summary="제주 지역 카페 ID 수집",
    description="200m 격자 단위로 나눈 제주도 좌표 데이터를 기반으로 카카오 API를 호출하여 주변 카페 ID를 수집하고 이를 DB에 저장합니다."
)
def crawl_cafe_searches():
    """
    고정된 CSV(grid rects) 기반으로 전체 제주 지역 카페 ID를 수집하여 DB에 저장합니다.
    """
    result = run_grid_crawling()
    return result