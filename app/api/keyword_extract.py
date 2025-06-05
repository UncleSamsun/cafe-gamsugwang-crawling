from uuid import uuid4
from app.core.redis_client import get_redis
from fastapi import HTTPException, status
from fastapi import APIRouter, HTTPException, BackgroundTasks

from app.service.keyword_clustering import cluster_keywords_per_cafe
from app.service.keyword_extractor import extract_all_keywords
from app.service.keyword_extract_job import extract_and_cluster_job

router = APIRouter()

@router.post(
    "/",
    summary="모든 카페 리뷰 키워드 추출 및 클러스터링",
    description="모든 카페의 리뷰 데이터를 분석하여 키워드를 추출하고, 이를 클러스터링하여 대표 키워드를 도출합니다."
)
async def extract_keywords(background_tasks: BackgroundTasks):
    job_id = str(uuid4())
    redis = get_redis()
    redis.hset(
        f"keyword_extract_job:{job_id}",
        mapping={"status": "in_progress", "progress": "0", "stage": "", "error": ""}
    )
    background_tasks.add_task(extract_and_cluster_job, job_id)
    return {"job_id": job_id}


@router.get(
    "/{job_id}",
    summary="키워드 추출 상태 조회",
    description="주어진 job_id에 해당하는 키워드 추출 및 클러스터링 작업의 상태를 조회합니다."
)
async def get_extract_status(job_id: str):
    redis = get_redis()
    data = redis.hgetall(f"keyword_extract_job:{job_id}")
    if not data:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found")
    return {
        "status": data.get("status", ""),
        "progress": data.get("progress", ""),
        "stage": data.get("stage", ""),
        "error": data.get("error", ""),
    }