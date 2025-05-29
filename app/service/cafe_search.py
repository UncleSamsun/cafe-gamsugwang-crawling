""" 
이 스크립트는 카카오 로컬 API를 이용하여 지정된 지리적 영역 내 카페 정보를 검색하고, 
수집된 카페 데이터를 데이터베이스에 저장하는 기능을 수행합니다. 
주요 기능으로는 API 요청 세션 생성, 카페 검색, 결과 저장, 그리고 그리드 기반 크롤링 실행이 포함되어 있습니다.
"""

import requests
import pandas as pd
import time
import json
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
import os
from app.core.db import get_connection


def create_session():
    """
    재시도 정책이 적용된 requests 세션을 생성합니다.
    네트워크 오류 발생 시 자동으로 재시도하며, 
    안정적인 API 호출을 위해 HTTPAdapter를 설정합니다.
    """
    session = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=0.5,
        status_forcelist=[500, 502, 503, 504]
    )
    session.mount('https://', HTTPAdapter(max_retries=retries))
    return session


def search_cafes(min_lat, min_lng, max_lat, max_lng, api_key, session):
    """
    지정된 좌표 범위 내에서 카카오 로컬 API를 이용해 카페 정보를 검색합니다.
    
    Args:
        min_lat (float): 최소 위도
        min_lng (float): 최소 경도
        max_lat (float): 최대 위도
        max_lng (float): 최대 경도
        api_key (str): 카카오 API 키
        session (requests.Session): 재사용 가능한 HTTP 세션 객체
    
    Returns:
        list[dict]: 검색된 카페 정보 리스트
    """
    headers = {
        "Authorization": f"KakaoAK {api_key}",
        "Content-Type": "application/json; charset=utf-8"
    }

    cafe_ids = set()
    cafe_data = []
    page = 1

    while True:
        try:
            # API 호출: 카테고리별 카페 검색 요청 구성
            url = "https://dapi.kakao.com/v2/local/search/category.json"
            params = {
                "category_group_code": "CE7",  # 카페 카테고리 코드
                "rect": f"{min_lng},{min_lat},{max_lng},{max_lat}",
                "page": page,
                "size": 15
            }

            print(f"API 요청 중: 페이지 {page}")

            response = session.get(url, headers=headers, params=params)

            # API 키 인증 오류 처리
            if response.status_code in (401, 403):
                print(f"API 키 인증 오류 발생 (상태 코드: {response.status_code})")
                print("새로운 API 키를 발급받아 사용해주세요.")
                return cafe_data

            response.raise_for_status()
            result = response.json()

            # 응답에서 카페 데이터 추출
            documents = result.get("documents", [])
            if not documents:
                print("더 이상 검색 결과가 없습니다.")
                break

            for document in documents:
                cafe_id = document.get("id")
                place_name = document.get("place_name")
                x = document.get("x")
                y = document.get("y")
                if cafe_id and place_name and x and y:
                    if cafe_id not in cafe_ids:
                        cafe_ids.add(cafe_id)
                        cafe_data.append({
                            "cafe_id": cafe_id,
                            "place_name": place_name,
                            "x": float(x),
                            "y": float(y)
                        })

            print(f"{page}페이지 완료: 총 {len(cafe_ids)}개 카페 수집됨")

            # 다음 페이지가 없으면 종료
            if result.get("meta", {}).get("is_end", True):
                print("마지막 페이지에 도달했습니다.")
                break
            else:
                page += 1
                time.sleep(1)  # API 호출 간격 조절

        except requests.exceptions.RequestException as e:
            # 네트워크 오류 처리 및 재시도
            print(f"네트워크 오류 발생: {e}")
            time.sleep(2)
            continue
        except Exception as e:
            # 기타 예상치 못한 오류 처리
            print(f"예상치 못한 오류 발생: {e}")
            break

    return cafe_data


def save_results(results, filename):
    """
    수집된 결과를 JSON 파일로 저장합니다.
    
    Args:
        results (list[dict]): 저장할 데이터 리스트
        filename (str): 저장할 파일 경로
    """
    try:
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        print(f"결과가 성공적으로 저장되었습니다: {filename}")
    except Exception as e:
        print(f"결과 저장 중 오류 발생: {e}")


def save_cafe_ids(grid_key: str, cafe_data: list[dict]):
    """
    수집된 카페 정보를 데이터베이스에 저장합니다.
    중복된 ID는 무시하며, 저장 후 전체 저장된 카페 수를 출력합니다.
    
    Args:
        grid_key (str): 현재 그리드 영역 식별 키
        cafe_data (list[dict]): 저장할 카페 데이터 리스트
    """
    if not cafe_data:
        print(f"{grid_key} 영역에서 수집된 카페 정보가 없어 저장을 생략합니다.")
        return

    conn = get_connection()
    cursor = conn.cursor()
    for cafe in cafe_data:
        try:
            cursor.execute(
                "INSERT IGNORE INTO cafe_ids (id, place_name, x, y) VALUES (%s, %s, %s, %s)",
                (cafe["cafe_id"], cafe["place_name"], cafe["x"], cafe["y"])
            )
        except Exception as e:
            print(f"❌ DB 삽입 실패: {e}")
    conn.commit()
    print(f"{grid_key} 영역 저장 완료 ({len(cafe_data)}개 데이터 저장됨)")

    cursor.execute("SELECT COUNT(*) AS total FROM cafe_ids")
    total = cursor.fetchone()["total"]
    print(f"현재까지 저장된 전체 카페 수: {total}")

    cursor.close()
    conn.close()


def run_grid_crawling():
    """
    그리드 형태로 분할된 영역별로 카페 정보를 크롤링하고 저장합니다.
    환경변수에서 API 키를 읽어오며, 각 영역별로 API를 호출하여 데이터를 수집합니다.
    
    Returns:
        dict: 저장된 고유 카페 ID 수를 포함하는 딕셔너리
    """
    API_KEY = os.getenv("KAKAO_API_KEY", "")
    if not API_KEY:
        raise EnvironmentError("KAKAO_API_KEY 환경변수가 설정되지 않았습니다.")

    session = create_session()
    place_ids = set()

    grid_rects = pd.read_csv("data/map/grid_jeju_rects_200m_filtered.csv")
    print(f"총 검색할 사각형 영역 수: {len(grid_rects)}")

    for idx, row in grid_rects.iterrows():
        min_lat, min_lng = row["min_lat"], row["min_lng"]
        max_lat, max_lng = row["max_lat"], row["max_lng"]
        grid_key = f"{min_lat:.6f},{min_lng:.6f},{max_lat:.6f},{max_lng:.6f}"

        print(f"\n[{idx + 1}/{len(grid_rects)}] 영역 검색 시작: {grid_key}")
        cafe_data = search_cafes(min_lat, min_lng, max_lat, max_lng, API_KEY, session)
        save_cafe_ids(grid_key, cafe_data)
        place_ids.update([cafe["cafe_id"] for cafe in cafe_data])

    print(f"\n전체 크롤링 완료: 총 {len(place_ids)}개의 고유 카페 ID 저장됨")
    return {"saved": len(place_ids)}


def main():
    """
    메인 함수: 그리드 크롤링을 실행하고 결과를 출력합니다.
    """
    result = run_grid_crawling()
    print(f"크롤링 결과: {result}")


if __name__ == "__main__":
    main()