import requests
import pandas as pd
import time
import json
from datetime import datetime
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import os
from service.db import get_connection


def create_session():
    session = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=0.5,
        status_forcelist=[500, 502, 503, 504]
    )
    session.mount('https://', HTTPAdapter(max_retries=retries))
    return session

def search_cafes(min_lat, min_lng, max_lat, max_lng, api_key, session):
    headers = {
        "Authorization": f"KakaoAK {api_key}",
        "Content-Type": "application/json; charset=utf-8"
    }

    cafe_ids = set()
    cafe_data = []
    page = 1

    while True:
        try:
            url = "https://dapi.kakao.com/v2/local/search/category.json"
            params = {
                "category_group_code": "CE7",
                "rect": f"{min_lng},{min_lat},{max_lng},{max_lat}",
                "page": page,
                "size": 15
            }

            print(f"    🔍 API 요청: 페이지 {page}")

            response = session.get(url, headers=headers, params=params)

            if response.status_code == 401 or response.status_code == 403:
                print(f"    ⚠️ API 키 인증 오류 (상태 코드: {response.status_code})")
                print("    💡 새로운 API 키를 발급받아 사용해주세요.")
                return cafe_data

            response.raise_for_status()
            result = response.json()

            documents = result.get("documents", [])
            if not documents:
                break

            for doc in documents:
                place_id = doc.get("id")
                place_name = doc.get("place_name")
                x = doc.get("x")
                y = doc.get("y")
                if place_id and place_name and x and y:
                    cafe_ids.add(place_id)
                    cafe_data.append({
                        "id": place_id,
                        "place_name": place_name,
                        "x": float(x),
                        "y": float(y)
                    })

            print(f"    📄 {page}페이지: 총 {len(cafe_ids)}개 수집됨")

            if not result.get("meta", {}).get("is_end", True):
                page += 1
                time.sleep(1)  # API 호출 간격 증가
            else:
                break

        except requests.exceptions.RequestException as e:
            print(f"    ⚠️ 네트워크 오류: {e}")
            time.sleep(2)  # 오류 발생 시 더 긴 대기 시간
            continue
        except Exception as e:
            print(f"    ⚠️ 예상치 못한 오류: {e}")
            break

    return cafe_data

def save_results(results, filename):
    try:
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        print(f"💾 결과 저장 완료: {filename}")
    except Exception as e:
        print(f"⚠️ 결과 저장 실패: {e}")

def save_cafe_ids(grid_key: str, cafe_data: list[dict]):
    if not cafe_data:
        print(f"⚠️ {grid_key} 영역에서 수집된 카페 ID가 없어 저장을 생략합니다.")
        return

    conn = get_connection()
    cursor = conn.cursor()
    for cafe in cafe_data:
        try:
            cursor.execute(
                "INSERT IGNORE INTO cafe_ids (id, place_name, x, y) VALUES (%s, %s, %s, %s)",
                (cafe["id"], cafe["place_name"], cafe["x"], cafe["y"])
            )
        except Exception as e:
            print(f"❌ DB INSERT 실패: {e}")
    conn.commit()
    print(f"✅ {grid_key} 저장 완료 ({len(cafe_data)}개)")
    cursor.execute("SELECT COUNT(*) AS total FROM cafe_ids")
    total = cursor.fetchone()["total"]
    print(f"📊 현재까지 저장된 전체 카페 ID 수: {total}")
    cursor.close()
    conn.close()

def main():
    API_KEY = os.getenv("KAKAO_API_KEY", "51877354ca225b32a1b388cbbd4d877f")
    start = datetime.now()
    print(f"🚀 시작 시간: {start.strftime('%Y-%m-%d %H:%M:%S')}")

    session = create_session()
    place_ids = set()

    try:
        grid_rects = pd.read_csv("../data/map/filtered_jeju_grid_rects_200m.csv")
        print(f"📦 검색 사각형 수: {len(grid_rects)}")

        results = {
            "metadata": {
                "start_time": start.strftime("%Y-%m-%d %H:%M:%S"),
                "total_grid_points": len(grid_rects),
                "is_test": False
            },
            "cafe_ids": {}
        }

        for idx, row in grid_rects.iterrows():
            min_lat, min_lng = row["min_lat"], row["min_lng"]
            max_lat, max_lng = row["max_lat"], row["max_lng"]
            grid_key = f"{min_lat:.6f},{min_lng:.6f},{max_lat:.6f},{max_lng:.6f}"

            print(f"\n🔍 검색 중: 셀 {idx + 1}/{len(grid_rects)} ({grid_key})")
            cafe_data = search_cafes(min_lat, min_lng, max_lat, max_lng, API_KEY, session)
            save_cafe_ids(grid_key, cafe_data)
            place_ids.update([cafe["id"] for cafe in cafe_data])

    except Exception as e:
        print(f"⚠️ 실행 중 오류 발생: {e}")
    finally:
        end = datetime.now()
        results["metadata"]["end_time"] = end.strftime("%Y-%m-%d %H:%M:%S")
        results["metadata"]["duration_seconds"] = (end - start).seconds

        print(f"✅ 종료 시간: {end.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"⏱️ 총 소요 시간: {(end - start).seconds}초")
        print(f"총 수집한 카페 ID 개수: {len(place_ids)}")

if __name__ == "__main__":
    main()

def run_grid_crawling():
    API_KEY = os.getenv("KAKAO_API_KEY", "51877354ca225b32a1b388cbbd4d877f")
    session = create_session()
    place_ids = set()

    grid_rects = pd.read_csv("data/map/filtered_jeju_grid_rects_200m.csv")
    print(f"📦 검색 사각형 수: {len(grid_rects)}")

    for idx, row in grid_rects.iterrows():
        min_lat, min_lng = row["min_lat"], row["min_lng"]
        max_lat, max_lng = row["max_lat"], row["max_lng"]
        grid_key = f"{min_lat:.6f},{min_lng:.6f},{max_lat:.6f},{max_lng:.6f}"

        print(f"\n🔍 검색 중: 셀 {idx + 1}/{len(grid_rects)} ({grid_key})")
        cafe_data = search_cafes(min_lat, min_lng, max_lat, max_lng, API_KEY, session)
        save_cafe_ids(grid_key, cafe_data)
        place_ids.update([cafe["id"] for cafe in cafe_data])

    return {"saved": len(place_ids)}