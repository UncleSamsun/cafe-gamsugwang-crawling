from fastapi import APIRouter
from service.crawl_cafe_details import crawl_and_save_all_cafes

router = APIRouter()

@router.post("/crawl-all")
def crawl_all_cafe_details():
    """
    모든 cafe_id에 대해 상세 정보 및 리뷰를 크롤링하고 DB에 저장합니다.
    """
    result = crawl_and_save_all_cafes()
    return result