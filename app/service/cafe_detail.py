"""
이 모듈은 카카오맵에서 특정 카페의 상세 정보를 크롤링하고,
수집한 데이터를 데이터베이스에 저장하는 기능을 제공합니다.
"""

import time
import re
import math
import asyncio
from concurrent.futures import ThreadPoolExecutor, as_completed
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service as ChromeService
import os
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from app.core.db import get_connection
from app.core.redis_client import get_redis

DEFAULT_WAIT = 5
SHORT_WAIT = 3

def crawl_and_save_single_cafe(cafe_id):
    """
    단일 카페 ID를 받아 카카오맵에서 상세 정보를 크롤링하고,
    수집한 데이터를 데이터베이스에 저장합니다.
    크롤링 실패 시 False를 반환합니다.
    """
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-software-rasterizer")
    # Specify Chromium binary location
    options.binary_location = "/usr/bin/chromium"
    service = ChromeService(executable_path="/usr/bin/chromedriver")
    driver = webdriver.Chrome(service=service, options=options)
    try:
        url = f"https://place.map.kakao.com/{cafe_id}"
        driver.get(url)

        # 페이지 로딩 및 기본 정보 로딩 대기
        try:
            WebDriverWait(driver, DEFAULT_WAIT).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )
            name_elem = WebDriverWait(driver, DEFAULT_WAIT * 2).until(
                EC.visibility_of_element_located((By.CSS_SELECTOR, "h3.tit_place"))
            )
            name = name_elem.text.strip()
        except:
            print(f"❌ {cafe_id} - 'tit_place' 요소 없음 (로딩 대기 후 실패)")
            driver.quit()
            return False

        # 주소 정보 수집
        try:
            address = WebDriverWait(driver, SHORT_WAIT).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "span.txt_detail"))
            ).text.strip()
        except:
            address = None

        # 전화번호 정보 수집
        phone = None
        try:
            phone_sections = WebDriverWait(driver, DEFAULT_WAIT).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div.unit_default"))
            )
        except:
            phone_sections = []
        for section in phone_sections:
            try:
                title_elem = section.find_element(By.CSS_SELECTOR, "h5.tit_info span.ico_call2")
                if title_elem and "전화" in title_elem.text:
                    phone = section.find_element(By.CSS_SELECTOR, "span.txt_detail").text.strip()
                    break
            except:
                continue

        # 영업시간 정보 확장 및 수집
        try:
            try:
                fold_buttons = driver.find_elements(By.CSS_SELECTOR, "button.btn_fold")
                for btn in fold_buttons:
                    if btn.get_attribute("aria-expanded") == "false":
                        btn.click()
                        WebDriverWait(driver, SHORT_WAIT).until(
                            lambda d: btn.get_attribute("aria-expanded") == "true"
                        )
            except:
                pass
            lines = WebDriverWait(driver, DEFAULT_WAIT).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div.line_fold"))
            )
            open_hours = []
            for line in lines:
                try:
                    day = line.find_element(By.CSS_SELECTOR, "span.tit_fold").text.strip()
                    detail = line.find_element(By.CSS_SELECTOR, "span.txt_detail").text.strip()
                    open_hours.append(f"{day}: {detail}")
                except:
                    continue
            open_time = "; ".join(open_hours)
        except:
            open_time = None

        # 평점 수집
        try:
            rating = float(WebDriverWait(driver, SHORT_WAIT).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "span.num_star"))
            ).text.strip())
        except:
            rating = 0.0

        # 리뷰 수집
        try:
            review_count = int(WebDriverWait(driver, SHORT_WAIT).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "span.info_num"))
            ).text.strip().replace("개", ""))
        except:
            review_count = 0

        # 평점과 리뷰 수의 일관성 확인
        if review_count == 0 and rating > 0.0:
            rating = 0.0

        # 메뉴 탭 클릭 및 메뉴 정보 수집
        try:
            menu_tab = driver.find_element(By.LINK_TEXT, "메뉴")
            menu_tab.click()
            time.sleep(1)
        except:
            pass
        try:
            menu_elements = WebDriverWait(driver, SHORT_WAIT).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, "ul.list_goods > li"))
            )
        except:
            menu_elements = []
        menus = []
        for m in menu_elements:
            try:
                menu_name = m.find_element(By.CSS_SELECTOR, "strong.tit_item").text.strip()
                menu_price_raw = m.find_element(By.CSS_SELECTOR, "p.desc_item").text.strip()
                menu_price = re.sub(r"[^\d]", "", menu_price_raw)
                try:
                    img_tag = m.find_element(By.CSS_SELECTOR, "img.img_goods")
                    menu_image_url = img_tag.get_attribute("src")
                    if menu_image_url.startswith("//"):
                        menu_image_url = "https:" + menu_image_url
                except:
                    menu_image_url = None
                menus.append((cafe_id, menu_name, int(menu_price), menu_image_url))
            except:
                continue

        # 우편번호 추출
        zipcode = None
        zipcode_match = re.search(r'\(우\)?(\d{5})', address if address else "")
        if zipcode_match:
            zipcode = zipcode_match.group(1)

        # 대표 이미지 URL 수집
        image_url = None
        try:
            thumb_img = WebDriverWait(driver, SHORT_WAIT).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "img.img-thumb.img_cfit"))
            )
            image_url = thumb_img.get_attribute("src")
            if image_url.startswith("//"):
                image_url = "https:" + image_url
        except:
            pass

        # DB에서 위치 정보 조회
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT x, y FROM cafe_ids WHERE id = %s", (cafe_id,))
        loc_result = cursor.fetchone()
        lon = loc_result["x"] if loc_result else None
        lat = loc_result["y"] if loc_result else None

        # 카페 정보 DB에 저장 (중복 시 업데이트)
        cursor.execute("""
            INSERT INTO cafes (id, title, address, open_time, rate, rate_count, image_url, zipcode, phone_number, lat, lon)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE title=VALUES(title), address=VALUES(address), open_time=VALUES(open_time), rate=VALUES(rate),
            rate_count=VALUES(rate_count), image_url=VALUES(image_url), zipcode=VALUES(zipcode),
            phone_number=VALUES(phone_number), lat=VALUES(lat), lon=VALUES(lon)
        """, (cafe_id, name, address, open_time, rating, review_count, image_url, zipcode, phone, lat, lon))

        # 후기 탭 클릭 및 후기 정보 수집
        try:
            review_tab = driver.find_element(By.LINK_TEXT, "후기")
            review_tab.click()
            time.sleep(1)
            last_height = driver.execute_script("return document.body.scrollHeight")
            while True:
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(1)
                new_height = driver.execute_script("return document.body.scrollHeight")
                if new_height == last_height:
                    break
                last_height = new_height
        except:
            pass

        try:
            review_elements = WebDriverWait(driver, SHORT_WAIT).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, "ul.list_review > li"))
            )
        except:
            review_elements = []

        for el in review_elements:
            try:
                star_tag = el.find_element(By.CSS_SELECTOR, "div.info_grade > span.starred_grade > span.screen_out:nth-of-type(2)")
                star = float(star_tag.get_attribute("textContent").strip())
                try:
                    content_tag = el.find_element(By.CSS_SELECTOR, "div.wrap_review p.desc_review")
                    content = content_tag.text.strip()
                    for suffix in ["더보기", "접기"]:
                        if content.endswith(suffix):
                            content = content[:-len(suffix)].strip()
                except:
                    continue
                cursor.execute("""
                    INSERT INTO kakao_reviews (cafe_id, content, rating)
                    VALUES (%s, %s, %s)
                """, (cafe_id, content, star))
            except:
                continue

        # 메뉴 정보 DB에 저장
        for m in menus:
            cursor.execute("""
                INSERT INTO menus (cafe_id, name, price, menu_image_url)
                VALUES (%s, %s, %s, %s)
            """, m)

        # 커밋 및 연결 종료
        conn.commit()
        cursor.close()
        conn.close()

        print(f"✅ cafeId:{cafe_id} 저장 완료")
        driver.quit()
        return True

    except Exception as e:
        print(f"❌ cafeId:{cafe_id} 처리 중 오류: {e}")
        driver.quit()
        return False


def crawl_all_cafes(job_id: str, update_progress_callback):
    """
    데이터베이스에 저장된 모든 카페 ID를 조회하여,
    각 카페의 상세 정보를 크롤링하고 저장합니다.
    실패한 카페 ID는 재시도하며 최종 결과를 반환합니다.
    """
    # 기존 데이터 삭제 및 초기화
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM kakao_reviews")
    cursor.execute("DELETE FROM menus")
    cursor.execute("DELETE FROM keywords")
    cursor.execute("DELETE FROM cafes")
    cursor.execute("ALTER TABLE kakao_reviews AUTO_INCREMENT = 1")
    cursor.execute("ALTER TABLE menus AUTO_INCREMENT = 1")
    cursor.execute("ALTER TABLE keywords AUTO_INCREMENT = 1")
    cursor.execute("ALTER TABLE cafes AUTO_INCREMENT = 1")
    conn.commit()

    start_time = time.time()

    # 모든 카페 ID 조회
    cursor.execute("SELECT DISTINCT id FROM cafe_ids")
    cafe_ids = [row["id"] for row in cursor.fetchall()]
    cursor.close()
    conn.close()

    total_ids = len(cafe_ids)
    processed_count = 0

    print(f"총 {total_ids}개의 카페 ID를 수집했습니다.")

    saved_count = 0
    failed_ids = []

    # 병렬로 크롤링 수행
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(crawl_and_save_single_cafe, cafe_id): cafe_id for cafe_id in cafe_ids}
        for future in as_completed(futures):
            cafe_id = futures[future]
            result = future.result()
            processed_count += 1
            percent = int(processed_count / total_ids * 100)
            update_progress_callback(percent, f"detail_step_{processed_count}")
            if result:
                saved_count += 1
            else:
                failed_ids.append(cafe_id)

    # 실패한 항목 재시도
    if failed_ids:
        print(f"🔁 {len(failed_ids)}개 항목 재시도 중...")
        with ThreadPoolExecutor(max_workers=5) as retry_executor:
            retry_futures = {retry_executor.submit(crawl_and_save_single_cafe, cafe_id): cafe_id for cafe_id in failed_ids}
            for future in as_completed(retry_futures):
                cafe_id = retry_futures[future]
                result = future.result()
                processed_count += 1
                percent = int(processed_count / total_ids * 100)
                update_progress_callback(percent, f"detail_step_{processed_count}")
                if result:
                    saved_count += 1
                    failed_ids.remove(cafe_id)

    elapsed_time = time.time() - start_time
    print(f"⏱ 크롤링 완료 - 소요 시간: {elapsed_time:.2f}초")
    print(f"✅ 저장된 카페 수: {saved_count} / {total_ids}")
    if failed_ids:
        print("❌ 실패한 카페 ID 목록:")
        print(", ".join(map(str, failed_ids)))

    update_progress_callback(100, "completed")
    return {"crawled_cafes": saved_count, "failed_ids": failed_ids}


async def cafe_detail_job(job_id: str, update_progress_callback: callable):
    """
    Background task wrapper to run crawl_all_cafes in a thread.
    """
    try:
        await asyncio.to_thread(crawl_all_cafes, job_id, update_progress_callback)
    except Exception as e:
        # on error, let caller handle setting failure status
        raise e