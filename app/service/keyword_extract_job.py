# app/service/keyword_extract_job.py

import asyncio
import traceback
from app.core.redis_client import get_redis
from app.service.keyword_extractor import extract_all_keywords
from app.service.keyword_clustering import cluster_keywords_per_cafe

async def extract_and_cluster_job(job_id: str):
    """
    백그라운드 작업: 키워드 추출 및 클러스터링을 수행하며, Redis에 진행률·상태를 업데이트합니다.
    """
    redis = get_redis()
    def update_progress_callback(progress: int, stage: str = ""):
        redis.hset(f"keyword_extract_job:{job_id}", mapping={
            "progress": str(progress),
            "stage": stage,
        })

    try:
        # 1) 전체 키워드 추출 (blocking 함수라 to_thread 사용)
        await asyncio.to_thread(extract_all_keywords, update_progress_callback)

        # 2) 클러스터링 수행 (blocking 함수라 to_thread 사용)
        await asyncio.to_thread(cluster_keywords_per_cafe, update_progress_callback)

        # 완료 시 상태 갱신
        redis.hset(f"keyword_extract_job:{job_id}", mapping={"status": "completed"})
    except Exception as e:
        traceback.print_exc()
        # 실패 시 상태 및 에러 메시지 갱신
        redis.hset(f"keyword_extract_job:{job_id}", mapping={
            "status": "failed",
            "error": str(e),
        })