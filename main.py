import uvicorn
from fastapi import FastAPI
from routers import cafe_search, cafe_detail, keyword_extract

app = FastAPI()

app.include_router(cafe_search.router, prefix="/cafe/search", tags=["Cafe Search"])
app.include_router(cafe_detail.router, prefix="/cafe/detail", tags=["Cafe Detail"])
# app.include_router(keyword_extract.router, prefix="/keywords", tags=["Keyword Extract"])

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)