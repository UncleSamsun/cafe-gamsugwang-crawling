from fastapi import APIRouter
from app.service.cafe_detail import crawl_all_cafes

router = APIRouter()

@router.post(
    "/crawl-all",
    summary="모든 카페 상세 정보 및 리뷰 크롤링",
    description="저장된 모든 cafe_id를 기반으로 카카오맵에서 각 카페의 상세 정보와 리뷰 데이터를 크롤링하고, 이를 DB에 저장합니다."
)
def crawl_all_cafe_details():
    """
    저장된 모든 cafe_id에 대해 상세 정보 및 리뷰를 크롤링하고 DB에 저장합니다.
    """
    result = crawl_all_cafes()
    return result