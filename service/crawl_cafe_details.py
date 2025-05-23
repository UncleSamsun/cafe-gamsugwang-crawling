import time
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from service.db import get_connection

DEFAULT_WAIT = 5
SHORT_WAIT = 3

def crawl_single_cafe(id):
    options = Options()
    options.add_argument("--headless")
    driver = webdriver.Chrome(options=options)
    try:
        url = f"https://place.map.kakao.com/{id}"
        driver.get(url)
        try:
            # 페이지 렌더링 완료까지 대기
            WebDriverWait(driver, DEFAULT_WAIT).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )
            # tit_place 요소가 렌더링될 때까지 최대 10초 대기
            name_elem = WebDriverWait(driver, DEFAULT_WAIT * 2).until(
                EC.visibility_of_element_located((By.CSS_SELECTOR, "h3.tit_place"))
            )
            name = name_elem.text.strip()
        except:
            print(f"❌ {id} - 'tit_place' 요소 없음 (로딩 대기 후 실패)")
            driver.quit()
            return False
        try:
            address = WebDriverWait(driver, SHORT_WAIT).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "span.txt_detail"))
            ).text.strip()
        except:
            address = None
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
        try:
            # Expand all fold sections before collecting open hours
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
        try:
            rating = float(WebDriverWait(driver, SHORT_WAIT).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "span.num_star"))
            ).text.strip())
        except:
            rating = 0.0
        try:
            review_count = int(WebDriverWait(driver, SHORT_WAIT).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "span.info_num"))
            ).text.strip().replace("개", ""))
        except:
            review_count = 0
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
                menus.append((id, menu_name, int(menu_price), menu_image_url))
            except:
                continue
        zipcode = None
        zipcode_match = re.search(r'\(우\)?(\d{5})', address)
        if zipcode_match:
            zipcode = zipcode_match.group(1)
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
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT x, y FROM cafe_ids WHERE id = %s", (id,))
        loc_result = cursor.fetchone()
        x = loc_result["x"] if loc_result else None
        y = loc_result["y"] if loc_result else None
        cursor.execute("""
            INSERT INTO cafes (id, title, address, open_time, rate, rate_count, image_url, zipcode, phone_number, x, y)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE title=VALUES(title), address=VALUES(address), open_time=VALUES(open_time), rate=VALUES(rate),
            rate_count=VALUES(rate_count), image_url=VALUES(image_url), zipcode=VALUES(zipcode),
            phone_number=VALUES(phone_number), x=VALUES(x), y=VALUES(y)
        """, (id, name, address, open_time, rating, review_count, image_url, zipcode, phone, x, y))
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
                    INSERT INTO reviews (cafe_id, content, rating)
                    VALUES (%s, %s, %s)
                """, (id, content, star))
            except:
                continue
        for m in menus:
            cursor.execute("""
                INSERT INTO menu (cafe_id, name, price, menu_image_url)
                VALUES (%s, %s, %s, %s)
            """, m)
        conn.commit()
        cursor.close()
        conn.close()
        print(f"✅ {id} 저장 완료")
        driver.quit()
        return True
    except Exception as e:
        print(f"❌ {id} 처리 중 오류: {e}")
        driver.quit()
        return False

def crawl_and_save_all_cafes():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM reviews")
    cursor.execute("DELETE FROM menu")
    cursor.execute("DELETE FROM cafes")
    cursor.execute("ALTER TABLE reviews AUTO_INCREMENT = 1")
    cursor.execute("ALTER TABLE menu AUTO_INCREMENT = 1")
    cursor.execute("ALTER TABLE cafes AUTO_INCREMENT = 1")
    conn.commit()
    start_time = time.time()

    cursor.execute("SELECT DISTINCT id FROM cafe_ids")
    # cursor.execute("SELECT DISTINCT id FROM cafe_ids LIMIT 10")
    cafe_ids = [row["id"] for row in cursor.fetchall()]
    cursor.close()
    conn.close()

    print(f"🟡 총 {len(cafe_ids)}개의 카페 ID를 수집했습니다.")

    saved_count = 0
    failed_ids = []

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(crawl_single_cafe, id): id for id in cafe_ids}
        for future in as_completed(futures):
            id = futures[future]
            result = future.result()
            if result:
                saved_count += 1
            else:
                failed_ids.append(id)

    if failed_ids:
        print(f"🔁 {len(failed_ids)}개 항목 재시도 중...")
        with ThreadPoolExecutor(max_workers=5) as retry_executor:
            retry_futures = {retry_executor.submit(crawl_single_cafe, id): id for id in failed_ids}
            for future in as_completed(retry_futures):
                id = retry_futures[future]
                result = future.result()
                if result:
                    saved_count += 1

    elapsed_time = time.time() - start_time
    print(f"⏱ 크롤링 완료 - 소요 시간: {elapsed_time:.2f}초")
    print(f"✅ 저장된 카페 수: {saved_count} / {len(cafe_ids)}")
    if failed_ids:
        print("❌ 실패한 카페 ID 목록:")
        print(", ".join(map(str, failed_ids)))
    return {"crawled_cafes": saved_count, "failed_ids": failed_ids}