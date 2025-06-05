from kiwipiepy import Kiwi
from keybert import KeyBERT
from app.core.db import get_connection
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from sentence_transformers import SentenceTransformer
korean_model = SentenceTransformer("snunlp/KR-SBERT-V40K-klueNLI-augSTS")
# model = KeyBERT(model=korean_model)

"""
이 모듈은 카페 리뷰에서 의미 있는 키워드를 추출하여 extracted_keywords 테이블에 저장하는 기능을 제공합니다.
Kiwi 형태소 분석기와 KeyBERT, SBERT 모델을 활용하여 리뷰 내용에서 중요한 단어들을 선별하고,
각 카페별로 키워드 빈도를 집계하여 관리합니다.
"""

def extract_all_keywords(update_progress_callback=None):
    """
    모든 카페의 리뷰 데이터를 분석하여 키워드를 추출하고 데이터베이스에 저장하는 함수입니다.

    주요 기능:
    1. 기존 extracted_keywords 테이블의 데이터를 삭제하고 AUTO_INCREMENT를 초기화합니다.
    2. cafes 테이블에서 모든 카페 정보를 조회합니다.
    3. 각 카페별로 kakao_reviews 테이블에서 리뷰 내용을 가져와 형태소 분석을 수행합니다.
    4. 불용어 및 의미 없는 단어를 필터링하여 키워드를 선별합니다.
    5. 선별된 키워드의 빈도를 extracted_keywords 테이블에 갱신하거나 신규 삽입합니다.
    6. 처리 진행 상황을 로깅하며, 오류 발생 시 롤백 처리합니다.

    반환값:
        처리한 카페 수 (int)
    """
    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            # 기존 키워드 삭제 및 AUTO_INCREMENT 초기화
            cursor.execute("DELETE FROM extracted_keywords")
            cursor.execute("ALTER TABLE extracted_keywords AUTO_INCREMENT = 1")

            # 모든 카페 ID 조회
            cursor.execute("SELECT id FROM cafes")
            cafes = cursor.fetchall()
            logger.info(f"{len(cafes)}개의 카페에 대해 키워드 추출을 시작합니다.")
            total_cafes = len(cafes)
            processed_cafes = 0

            count_total = 0
            # Kiwi 형태소 분석기 초기화
            kiwi = Kiwi()

            for cafe in cafes:
                cafe_id = cafe["id"]
                # 해당 카페의 모든 리뷰 내용 조회
                cursor.execute("SELECT content FROM kakao_reviews WHERE cafe_id = %s", (cafe_id,))
                reviews = cursor.fetchall()
                logger.info(f"카페 ID {cafe_id} 처리 중 - 리뷰 {len(reviews)}건")

                for review in reviews:
                    if not review["content"]:
                        # 리뷰 내용이 없으면 건너뜀
                        continue

                    # 리뷰 내용을 형태소 분석하여 토큰 추출
                    tokens = kiwi.analyze(review["content"])[0][0]

                    extracted_words = set()
                    stopwords = {
                        # 일반 동사/보조동사
                        "가다", "오다", "되다", "하다", "있다", "없다", "보다", "보이다", "보여주다",
                        "들다", "나다", "타다", "계시다", "살다", "사다", "받다", "내다", "주다",
                        "오르다", "내리다", "열다", "닫다", "나오다", "들어가다", "들어오다", "지나다", "끝나다",
                        "드리다", "드시다", "올리다", "내려가다", "가지다", "갖다", "넣다", "빠지다",
                        "찍다", "쓰다", "따르다", "버리다", "사용하다", "걸다", "놓다",

                        # 너무 일반적인 추상 명사
                        "사람", "일", "것", "때", "거", "좀", "뭔가", "누구", "다른", "다시", "항상", "그냥",
                        "서비스", "사진", "위치", "가게", "문제", "기본", "직원", "고객", "테이블",
                        "가격", "매장", "제품", "카페",

                        # 감탄사 및 의미 없는 표현
                        "아", "야", "음", "어", "응", "헐", "흠", "헉", "ㅋㅋ", "ㅎㅎ", "ㅠㅠ", "ㅜㅜ",

                        # 기타 노이즈
                        "진짜", "완전", "정말", "너무", "많이", "약간", "좀", "계속", "또", "많다", "조금", "되게", "대박",

                        # 일반 형용사/감정 표현 필터
                        "좋다", "괜찮다", "그렇다"
                    }

                    # 형태소 분석 결과에서 의미 있는 단어만 선별
                    for token in tokens:
                        if token.form in stopwords:
                            continue
                        if len(token.form) < 2:
                            continue
                        # 명사(NNG, NNP) 또는 형용사/동사(VA, VV)인 경우 처리
                        if token.tag in {'NNG', 'NNP'}:
                            extracted_words.add(token.form)
                        elif token.tag.startswith("VA") or token.tag.startswith("VV"):
                            if token.lemma in stopwords:
                                continue
                            if len(token.lemma) < 2:
                                continue
                            extracted_words.add(token.lemma)

                    # 추출된 키워드별로 데이터베이스에 존재 여부 확인 후 갱신 또는 삽입
                    for keyword in extracted_words:
                        if keyword in stopwords:
                            continue
                        cursor.execute("""
                            SELECT count FROM extracted_keywords WHERE cafe_id = %s AND keyword = %s
                        """, (cafe_id, keyword))
                        existing = cursor.fetchone()

                        if existing:
                            # 이미 존재하면 count 증가
                            cursor.execute("""
                                UPDATE extracted_keywords SET count = count + 1 WHERE cafe_id = %s AND keyword = %s
                            """, (cafe_id, keyword))
                        else:
                            # 새 키워드면 삽입
                            cursor.execute("""
                                INSERT INTO extracted_keywords (cafe_id, keyword, count) VALUES (%s, %s, 1)
                            """, (cafe_id, keyword))

                processed_cafes += 1
                if update_progress_callback:
                    percent = int(processed_cafes / total_cafes * 50)
                    update_progress_callback(percent, f"extracting_cafe_{processed_cafes}")
                count_total += 1

            if update_progress_callback:
                update_progress_callback(50, "extraction_completed")
            conn.commit()
            logger.info(f"{count_total}개의 카페에 대해 키워드 추출을 완료했습니다.")
            return count_total

    except Exception as e:
        conn.rollback()
        logger.error(f"키워드 추출 중 오류 발생: {e}", exc_info=True)
        raise
    finally:
        conn.close()
