"""
이 모듈은 카페별로 키워드를 클러스터링하고, 각 클러스터에서 대표 키워드를 추출하여 데이터베이스에 저장하는 기능을 제공합니다.
"""

from collections import defaultdict, Counter
from hdbscan import HDBSCAN
from app.core.db import get_connection
from sentence_transformers import SentenceTransformer
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_distances


def reset_cluster_tables():
    # 클러스터링 결과 저장 테이블 초기화
    conn = get_connection()
    with conn.cursor() as cursor:
        cursor.execute("DELETE FROM clustered_keywords")
        cursor.execute("DELETE FROM cluster_summaries")
        cursor.execute("ALTER TABLE clustered_keywords AUTO_INCREMENT = 1")
        cursor.execute("ALTER TABLE cluster_summaries AUTO_INCREMENT = 1")
    conn.commit()
    conn.close()


def fetch_keywords_grouped_by_cafe():
    # 데이터베이스에서 카페별 키워드 조회
    conn = get_connection()
    with conn.cursor() as cursor:
        cursor.execute("SELECT cafe_id, keyword FROM keywords")
        results = cursor.fetchall()
    conn.close()

    cafe_keywords = defaultdict(list)
    for row in results:
        cafe_keywords[row['cafe_id']].append(row['keyword'])
    return cafe_keywords


def embed_keywords(keywords, model):
    # 키워드 임베딩 생성
    return model.encode(keywords, show_progress_bar=True)


def save_clustered_keywords(cafe_id, cluster_labels, keywords):
    # 각 (cluster_id, keyword) 쌍의 빈도 계산 및 저장
    pair_counts = Counter()
    for keyword, label in zip(keywords, cluster_labels):
        if label == -1:
            continue  # 노이즈는 건너뜀
        pair_counts[(label, keyword)] += 1

    conn = get_connection()
    with conn.cursor() as cursor:
        for (label, keyword), count in pair_counts.items():
            cursor.execute(
                "INSERT INTO clustered_keywords (cafe_id, cluster_id, keyword, count) VALUES (%s, %s, %s, %s)",
                (cafe_id, label, keyword, count)
            )
        conn.commit()
    conn.close()


def extract_representative_keywords(cafe_id, cluster_labels, keywords, embeddings, tfidf_scores_per_cafe):
    # 클러스터별 대표 키워드 추출
    cluster_to_keywords = defaultdict(list)
    cluster_to_vectors = defaultdict(list)

    for keyword, label, vector in zip(keywords, cluster_labels, embeddings):
        if label == -1:
            continue
        cluster_to_keywords[label].append(keyword)
        cluster_to_vectors[label].append(vector)

    representative_data = []
    for cluster_id, vectors in cluster_to_vectors.items():
        center = np.mean(vectors, axis=0).reshape(1, -1)
        distances = cosine_distances(vectors, center).flatten()

        # 해당 클러스터 내 키워드들의 TF-IDF 점수 가져오기
        cluster_keywords = cluster_to_keywords[cluster_id]
        cluster_tfidf = np.array([tfidf_scores_per_cafe.get(kw, 0) for kw in cluster_keywords])

        # 거리와 TF-IDF 점수를 [0, 1] 범위로 정규화
        if len(distances) > 1 and distances.max() != distances.min():
            norm_distances = (distances - distances.min()) / (distances.max() - distances.min())
        else:
            norm_distances = np.zeros_like(distances)
        if len(cluster_tfidf) > 1 and cluster_tfidf.max() != cluster_tfidf.min():
            norm_tfidf = (cluster_tfidf - cluster_tfidf.min()) / (cluster_tfidf.max() - cluster_tfidf.min())
        else:
            norm_tfidf = np.zeros_like(cluster_tfidf)

        combined_score = 0.5 * norm_tfidf + 0.5 * (1 - norm_distances)
        max_index = np.argmax(combined_score)
        representative_keyword = cluster_keywords[max_index]
        count = len(cluster_keywords)
        representative_data.append((cafe_id, cluster_id, representative_keyword, count))

    return representative_data


def save_cluster_summary(representative_data, cafe_id, cluster_labels, keywords):
    # 각 (cafe_id, cluster_id)별 키워드 등장 횟수 계산 및 요약 저장
    cluster_keyword_count = defaultdict(int)
    for kw, label in zip(keywords, cluster_labels):
        if label != -1:
            cluster_keyword_count[(cafe_id, label)] += 1

    conn = get_connection()
    with conn.cursor() as cursor:
        for cafe_id, cluster_id, rep_keyword, _ in representative_data:
            count = cluster_keyword_count[(cafe_id, cluster_id)]
            cursor.execute(
                "INSERT INTO cluster_summaries (cafe_id, cluster_id, representative_keyword, keyword_count) VALUES (%s, %s, %s, %s)",
                (cafe_id, cluster_id, rep_keyword, count)
            )
        conn.commit()
    conn.close()


def cluster_keywords_per_cafe(min_cluster_size=2):
    # 카페별 키워드 클러스터링 메인 함수
    print("✅ 클러스터링 시작")
    reset_cluster_tables()
    print("✅ 테이블 리셋 완료")

    model = SentenceTransformer("snunlp/KR-SBERT-V40K-klueNLI-augSTS")
    print("✅ 모델 로딩 완료")

    cafe_keywords_map = fetch_keywords_grouped_by_cafe()
    print(f"✅ 키워드 수집 완료 - 카페 수: {len(cafe_keywords_map)}")

    # 모든 키워드를 수집하여 TF-IDF 벡터라이저 학습
    all_keywords = []
    for keywords in cafe_keywords_map.values():
        all_keywords.extend(keywords)
    tfidf_vectorizer = TfidfVectorizer()
    tfidf_vectorizer.fit(all_keywords)

    for cafe_id, keywords in cafe_keywords_map.items():
        try:
            print(f"▶️ {cafe_id}: {len(keywords)}개 키워드 클러스터링 중")
            if len(keywords) <= 2:
                print(f"⚠️ {cafe_id}: 키워드 수 부족")
                continue
            embeddings = embed_keywords(keywords, model)
            clusterer = HDBSCAN(min_cluster_size=min_cluster_size, min_samples=1, cluster_selection_epsilon=0.1)
            cluster_labels = clusterer.fit_predict(embeddings)

            # 해당 카페 키워드에 대한 TF-IDF 점수 계산
            tfidf_scores_per_cafe = {}
            tfidf_matrix = tfidf_vectorizer.transform(keywords)
            for idx, kw in enumerate(keywords):
                # 키워드 내 모든 토큰의 TF-IDF 점수 합산
                tfidf_scores_per_cafe[kw] = tfidf_matrix[idx].sum()

            save_clustered_keywords(cafe_id, cluster_labels, keywords)
            representative_data = extract_representative_keywords(cafe_id, cluster_labels, keywords, embeddings, tfidf_scores_per_cafe)
            save_cluster_summary(representative_data, cafe_id, cluster_labels, keywords)
            print(f"✅ {cafe_id}: 클러스터링 완료")
        except Exception as e:
            print(f"❌ {cafe_id} 처리 중 오류 발생: {str(e)}")