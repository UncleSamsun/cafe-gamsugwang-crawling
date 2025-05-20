from fastapi import APIRouter
from service.crawl_runner import run_grid_crawling

router = APIRouter()

@router.get("/")
def search_cafes():
    """
    고정된 CSV(grid rects) 기반으로 전체 제주 지역 카페 ID를 수집하여 DB에 저장합니다.
    """
    result = run_grid_crawling()
    return result