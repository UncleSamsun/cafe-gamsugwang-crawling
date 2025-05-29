from fastapi import APIRouter, HTTPException

from app.service.keyword_clustering import cluster_keywords_per_cafe
from app.service.keyword_extractor import extract_all_keywords

router = APIRouter()

@router.post(
    "/extract/all",
    summary="모든 카페 리뷰 키워드 추출 및 클러스터링",
    description="모든 카페의 리뷰 데이터를 분석하여 키워드를 추출하고, 이를 클러스터링하여 대표 키워드를 도출합니다."
)
def extract_keywords():
    try:
        extracted_count = extract_all_keywords()
        cluster_keywords_per_cafe()
        return {"message": f"{extracted_count} cafes processed and keywords clustered."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))