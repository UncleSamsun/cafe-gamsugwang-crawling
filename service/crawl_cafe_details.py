import time
import re
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from service.db import get_connection

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

    cursor.execute("SELECT DISTINCT cafe_id FROM cafe_ids")
    # cursor.execute("SELECT DISTINCT id FROM cafe_ids LIMIT 10")
    cafe_ids = [row["id"] for row in cursor.fetchall()]
    cursor.close()
    conn.close()

    print(f"🟡 총 {len(cafe_ids)}개의 카페 ID를 수집했습니다.")

    options = Options()
    options.add_argument("--headless")
    driver = webdriver.Chrome(options=options)

    saved_count = 0
    for id in cafe_ids:
        try:
            url = f"https://place.map.kakao.com/{id}"
            driver.get(url)
            time.sleep(1)

            # 실제 정보 추출
            name = driver.find_element(By.CSS_SELECTOR, "h3.tit_place").text.strip()
            address = driver.find_element(By.CSS_SELECTOR, "span.txt_detail").text.strip()

            # 전화번호 추출 로직 (제대로 된 div.unit_default 내 "전화" 항목에서만 추출)
            try:
                phone = None
                phone_sections = driver.find_elements(By.CSS_SELECTOR, "div.unit_default")
                for section in phone_sections:
                    try:
                        title_elem = section.find_element(By.CSS_SELECTOR, "h5.tit_info span.ico_call2")
                        if title_elem and "전화" in title_elem.text:
                            phone = section.find_element(By.CSS_SELECTOR, "span.txt_detail").text.strip()
                            break
                    except:
                        continue
            except:
                phone = None

            try:
                open_hours = driver.find_element(By.CSS_SELECTOR, "div.line_fold span.txt_detail").text.strip()
            except:
                open_hours = None

            try:
                rating = float(driver.find_element(By.CSS_SELECTOR, "span.num_star").text.strip())
            except:
                rating = 0.0

            try:
                review_count = int(driver.find_element(By.CSS_SELECTOR, "span.info_num").text.strip().replace("개", ""))
            except:
                review_count = 0

            # 메뉴탭 클릭 시도
            try:
                menu_tab = driver.find_element(By.LINK_TEXT, "메뉴")
                menu_tab.click()
                time.sleep(1)
            except:
                pass

            # 메뉴 수집
            menus = []
            menu_elements = driver.find_elements(By.CSS_SELECTOR, "ul.list_goods > li")
            for m in menu_elements:
                try:
                    menu_name = m.find_element(By.CSS_SELECTOR, "strong.tit_item").text.strip()
                    menu_price_raw = m.find_element(By.CSS_SELECTOR, "p.desc_item").text.strip()
                    menu_price = re.sub(r"[^\d]", "", menu_price_raw)

                    # 이미지가 없는 메뉴도 허용
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

            # Extract zipcode from address
            zipcode = None
            zipcode_match = re.search(r'\(우\)?(\d{5})', address)
            if zipcode_match:
                zipcode = zipcode_match.group(1)

            # 대표 이미지 추출
            image_url = None
            try:
                thumb_img = driver.find_element(By.CSS_SELECTOR, "img.img-thumb.img_cfit")
                image_url = thumb_img.get_attribute("src")
                if image_url.startswith("//"):
                    image_url = "https:" + image_url
            except:
                pass

            conn = get_connection()
            cursor = conn.cursor()

            # Get x and y from cafe_ids
            cursor.execute("SELECT x, y FROM cafe_ids WHERE id = %s", (id,))
            loc_result = cursor.fetchone()
            x = loc_result["x"] if loc_result else None
            y = loc_result["y"] if loc_result else None

            cursor.execute("""
                INSERT INTO cafes (id, title, address, rate, rate_count, image_url, zipcode, phone_number, x, y)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE title=VALUES(title), address=VALUES(address), rate=VALUES(rate),
                rate_count=VALUES(rate_count), image_url=VALUES(image_url), zipcode=VALUES(zipcode),
                phone_number=VALUES(phone_number), x=VALUES(x), y=VALUES(y)
            """, (id, name, address, rating, review_count, image_url, zipcode, phone, x, y))

            try:
                review_tab = driver.find_element(By.LINK_TEXT, "후기")
                review_tab.click()
                time.sleep(1)
                # 무한 스크롤 처리
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

            review_elements = driver.find_elements(By.CSS_SELECTOR, "ul.list_review > li")
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

            # 메뉴 데이터 저장
            for m in menus:
                cursor.execute("""
                    INSERT INTO menu (cafe_id, name, price, menu_image_url)
                    VALUES (%s, %s, %s, %s)
                """, m)

            conn.commit()
            cursor.close()
            conn.close()

            saved_count += 1
            print(f"✅ {id} 저장 완료")

        except Exception as e:
            print(f"❌ {id} 처리 중 오류: {e}")
            continue

    driver.quit()
    return {"crawled_cafes": saved_count}