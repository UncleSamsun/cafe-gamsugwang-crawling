from fastapi import APIRouter, HTTPException
from service.extract_keywords import extract_keywords_for_all_cafes

router = APIRouter()

@router.post("/extract/all")
def extract_keywords_for_all():
    try:
        total = extract_keywords_for_all_cafes()
        return {"message": f"Keywords extracted for {total} cafes."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
