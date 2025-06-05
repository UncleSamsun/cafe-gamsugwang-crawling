import uuid

from fastapi import APIRouter
from fastapi import HTTPException, status
from fastapi import BackgroundTasks
import asyncio
from app.service.cafe_detail import crawl_all_cafes
from app.service.cafe_detail import cafe_detail_job
from app.core.redis_client import get_redis

router = APIRouter()

@router.post(
    "/detail",
    summary="모든 카페 상세 정보 및 리뷰 크롤링",
    description="저장된 모든 cafe_id를 기반으로 카카오맵에서 각 카페의 상세 정보와 리뷰 데이터를 크롤링하고, 이를 DB에 저장합니다."
)
async def crawl_all_cafe_details(background_tasks: BackgroundTasks):
    """
    저장된 모든 cafe_id에 대해 상세 정보 및 리뷰를 크롤링하고 DB에 저장합니다.
    """
    job_id = str(uuid.uuid4())
    redis = get_redis()
    redis.hset(
        f"cafe_detail_job:{job_id}",
        mapping={
            "status": "in_progress",
            "progress": "0",
            "stage": "",
            "error": ""
        }
    )
    background_tasks.add_task(cafe_detail_job, job_id)
    return {"job_id": job_id}

@router.get(
    "/detail/{job_id}",
    summary="크롤링 상태 조회",
    description="주어진 job_id에 해당하는 카페 상세 크롤링 진행 상태를 조회합니다."
)
async def get_crawl_all_status(job_id: str):
    redis = get_redis()
    data = redis.hgetall(f"cafe_detail_job:{job_id}")
    if not data:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found")
    return {
        "status": data.get("status", ""),
        "progress": data.get("progress", ""),
        "stage": data.get("stage", ""),
        "error": data.get("error", ""),
    }

async def cafe_detail_job(job_id: str):
    """
    Background task to perform detailed crawling and update job status in Redis.
    """
    redis = get_redis()

    def update_progress_callback(progress: int, stage: str = ""):
        redis.hset(
            f"cafe_detail_job:{job_id}",
            mapping={"progress": str(progress), "stage": stage}
        )

    try:
        await cafe_detail_job_inner(job_id, update_progress_callback)
        redis.hset(f"cafe_detail_job:{job_id}", mapping={"status": "completed"})
    except Exception as e:
        redis.hset(f"cafe_detail_job:{job_id}", mapping={
            "status": "failed",
            "error": str(e),
        })

async def cafe_detail_job_inner(job_id: str, update_progress_callback: callable):
    await asyncio.to_thread(crawl_all_cafes, job_id, update_progress_callback)