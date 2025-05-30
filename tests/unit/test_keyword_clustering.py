import pytest
from unittest.mock import patch, MagicMock
import numpy as np
from app.service.keyword_clustering import get_connection

import app.service.keyword_clustering as kc

"""
reset_cluster_tables 성공: 테이블 초기화 및 커밋/닫기 호출
"""
def test_reset_cluster_tables_success(mock_db_connection):
    mock_conn, mock_cursor = mock_db_connection
    with patch.object(kc, "get_connection", return_value=mock_conn):
        # 컨텍스트 매니저 모킹
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        kc.reset_cluster_tables()
    # DELETE 및 ALTER 쿼리가 실행되었는지 확인
    assert mock_cursor.execute.call_args_list[0][0][0].startswith("DELETE FROM clustered_keywords")
    assert mock_cursor.execute.call_args_list[1][0][0].startswith("DELETE FROM cluster_summaries")
    assert "AUTO_INCREMENT = 1" in mock_cursor.execute.call_args_list[2][0][0]
    assert "AUTO_INCREMENT = 1" in mock_cursor.execute.call_args_list[3][0][0]
    mock_conn.commit.assert_called_once()
    mock_conn.close.assert_called_once()

"""
fetch_keywords_grouped_by_cafe 성공: 카페별 키워드 dict 반환
"""
def test_fetch_keywords_grouped_by_cafe_success(mock_db_connection):
    mock_conn, mock_cursor = mock_db_connection
    rows = [
        {"cafe_id": 1, "keyword": "a"},
        {"cafe_id": 1, "keyword": "b"},
        {"cafe_id": 2, "keyword": "x"}
    ]
    mock_cursor.fetchall.return_value = rows
    with patch.object(kc, "get_connection", return_value=mock_conn):
        # 컨텍스트 매니저 모킹
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        result = kc.fetch_keywords_grouped_by_cafe()
    assert isinstance(result, dict)
    assert result[1] == ["a", "b"]
    assert result[2] == ["x"]
    mock_conn.close.assert_called_once()

"""
embed_keywords 성공: 모델.encode 호출 결과 반환
"""
def test_embed_keywords():
    fake_keywords = ["a", "b"]
    fake_embeddings = np.array([[1, 2], [3, 4]])
    mock_model = MagicMock()
    mock_model.encode.return_value = fake_embeddings
    result = kc.embed_keywords(fake_keywords, mock_model)
    assert np.array_equal(result, fake_embeddings)
    mock_model.encode.assert_called_once_with(fake_keywords, show_progress_bar=True)

"""
extract_representative_keywords()가 올바른 대표 키워드를 선택하는지 테스트합니다.
"""
def test_extract_representative_keywords():
    cafe_id = 1
    keywords = ["k1", "k2", "k3"]
    cluster_labels = [0, 0, 1]
    # embeddings: simple vectors
    embeddings = np.array([
        [0.0, 0.0],
        [1.0, 1.0],
        [0.5, 0.5]
    ])
    tfidf_scores = {"k1": 0.1, "k2": 0.9, "k3": 0.5}
    rep = kc.extract_representative_keywords(cafe_id, cluster_labels, keywords, embeddings, tfidf_scores)
    # cluster 0: keywords [k1,k2], rep should be k2 (higher tfidf)
    # cluster 1: single k3, rep k3
    assert (1, 0, "k2", 2) in rep
    assert (1, 1, "k3", 1) in rep

"""
save_clustered_keywords 성공: 올바른 INSERT 호출
"""
def test_save_clustered_keywords_success(mock_db_connection):
    mock_conn, mock_cursor = mock_db_connection
    cafe_id = 1
    keywords = ["a", "b", "c"]
    cluster_labels = [0, -1, 0]
    # 첫번 a->0, b noise, c->0 => counts: (0,a):1, (0,c):1
    with patch.object(kc, "get_connection", return_value=mock_conn):
        # 컨텍스트 매니저 모킹
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        kc.save_clustered_keywords(cafe_id, cluster_labels, keywords)
    # 두 번의 INSERT 호출이 있어야 함
    insert_calls = [c for c in mock_cursor.execute.call_args_list if "INSERT INTO clustered_keywords" in c[0][0]]
    assert len(insert_calls) == 2
    # 파라미터 확인
    args = insert_calls[0][0][1]
    assert args[0] == cafe_id
    assert args[1] == 0

"""
cluster_keywords_per_cafe()에서 키워드 수 부족(c <=2)이면 저장 함수가 호출되지 않는지 테스트합니다.
"""
def test_cluster_keywords_per_cafe_failure_min_size(monkeypatch, mock_db_connection):
    monkeypatch.setattr(kc, "TfidfVectorizer", lambda **kwargs: MagicMock(
        fit=lambda docs: None,
        fit_transform=lambda docs: np.zeros((len(docs), 1))
    ))
    mock_conn, mock_cursor = mock_db_connection
    # fetch_keywords return one cafe with <=2 keywords
    data = {1: ["a", "b"]}
    monkeypatch.setattr(kc, "reset_cluster_tables", lambda: None)
    monkeypatch.setattr(kc, "fetch_keywords_grouped_by_cafe", lambda: data)
    # 임베딩, clustering, save 함수 모킹
    monkeypatch.setattr(kc, "embed_keywords", lambda kws, model: np.zeros((2,2)))
    monkeypatch.setattr(kc, "save_clustered_keywords", lambda *args, **kwargs: (_ for _ in ()).throw(Exception("should not call")))
    # 실행 시 예외가 발생하지 않아야 함
    kc.cluster_keywords_per_cafe(min_cluster_size=3)