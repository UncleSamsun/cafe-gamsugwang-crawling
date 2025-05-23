from kiwipiepy import Kiwi
from keybert import KeyBERT
from transformers import AutoModel, AutoTokenizer
from service.db import get_connection
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from sentence_transformers import SentenceTransformer
korean_model = SentenceTransformer("snunlp/KR-SBERT-V40K-klueNLI-augSTS")
model = KeyBERT(model=korean_model)



def extract_keywords_for_all_cafes():
    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            # Delete all existing keywords and reset auto-increment
            cursor.execute("DELETE FROM keywords")
            cursor.execute("ALTER TABLE keywords AUTO_INCREMENT = 1")
            cursor.execute("SELECT id FROM cafes")
            # cursor.execute("SELECT id FROM cafes LIMIT 10")
            cafes = cursor.fetchall()
            count_total = 0
            kiwi = Kiwi()

            for cafe in cafes:
                cafe_id = cafe["id"]
                cursor.execute("SELECT content FROM reviews WHERE cafe_id = %s", (cafe_id,))
                reviews = cursor.fetchall()
                for r in reviews:
                    if not r["content"]:
                        continue
                    tokens = kiwi.analyze(r["content"])[0][0]
                    extracted_words = set()
                    stopwords = {
                        "가다", "오다", "보다", "되다", "하다", "들다", "먹다", "마시다", "주다", "받다",
                        "나다", "타다", "살다", "계시다", "없다", "있다", "올라오다", "내주다",
                        "올리다", "가지다", "넣다", "갖추다", "나오다", "끝나다", "드시다", "드리다",
                        "들어가다", "지나다", "들리다", "들어오다", "세우다", "사다", "찍다",
                        "따르다", "버리다", "사용하다"
                    }
                    for token in tokens:
                        if token.tag in {'NNG', 'NNP'}:
                            extracted_words.add(token.form)
                        elif token.tag.startswith("VA") or token.tag.startswith("VV"):
                            extracted_words.add(token.lemma)
                    for kw in extracted_words:
                        if kw in stopwords:
                            continue
                        if len(kw) < 2:
                            continue
                        cursor.execute("""
                            SELECT count FROM keywords WHERE cafe_id = %s AND keyword = %s
                        """, (cafe_id, kw))
                        existing = cursor.fetchone()

                        if existing:
                            cursor.execute("""
                                UPDATE keywords SET count = count + 1 WHERE cafe_id = %s AND keyword = %s
                            """, (cafe_id, kw))
                        else:
                            cursor.execute("""
                                INSERT INTO keywords (cafe_id, keyword, count) VALUES (%s, %s, 1)
                            """, (cafe_id, kw))
                count_total += 1

            conn.commit()
            logger.info(f"Extracted keywords for {count_total} cafes.")
            return count_total

    except Exception as e:
        conn.rollback()
        logger.error(f"Error during keyword extraction: {e}", exc_info=True)
        raise
    finally:
        conn.close()
