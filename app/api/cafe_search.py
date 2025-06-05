from fastapi import APIRouter, BackgroundTasks, HTTPException, status
import uuid
import asyncio
from app.service.cafe_search import run_grid_crawling
from app.core.redis_client import get_redis

router = APIRouter()


@router.post(
    "/search",
    summary="제주 지역 카페 ID 수집 작업 시작",
    description="200m 격자 단위로 나눈 제주도 좌표 데이터를 기반으로 카카오 API를 호출하여 주변 카페 ID를 수집하고 이를 DB에 저장하는 작업을 비동기로 시작합니다."
)
async def cafe_search(background_tasks: BackgroundTasks):
    """
    고정된 CSV(grid rects) 기반으로 전체 제주 지역 카페 ID를 수집하여 DB에 저장하는 작업을 비동기로 시작합니다.
    """
    job_id = str(uuid.uuid4())
    redis = get_redis()
    redis.hset(f"cafe_search_job:{job_id}", mapping={
        "status": "in_progress",
        "progress": "0",
        "stage": "",
        "error": "",
    })
    background_tasks.add_task(cafe_search_job, job_id)
    return {"job_id": job_id}

async def cafe_search_job(job_id: str):
    """
    Background task to perform grid crawling and update job status in Redis.
    """
    redis = get_redis()
    def update_progress_callback(progress: int, stage: str = ""):
        redis.hset(f"cafe_search_job:{job_id}", mapping={
            "progress": str(progress),
            "stage": stage,
        })
    try:
        await asyncio.to_thread(run_grid_crawling, job_id, update_progress_callback)
        redis.hset(f"cafe_search_job:{job_id}", mapping={"status": "completed"})
    except Exception as e:
        redis.hset(f"cafe_search_job:{job_id}", mapping={
            "status": "failed",
            "error": str(e),
        })


@router.get(
    "/search/{job_id}",
    summary="카페 ID 수집 작업 상태 조회",
    description="비동기로 실행 중인 제주 지역 카페 ID 수집 작업의 상태, 진행률, 에러 정보를 조회합니다."
)
async def get_cafe_search_job_status(job_id: str):
    redis = get_redis()
    data = redis.hgetall(f"cafe_search_job:{job_id}")
    if not data:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found")
    return {
        "status": data.get("status", ""),
        "progress": data.get("progress", ""),
        "stage": data.get("stage", ""),
        "error": data.get("error", ""),
    }