import pytest
from unittest.mock import patch, MagicMock
import app.service.keyword_extractor as ke_module
from app.service.keyword_extractor import get_connection

def make_mock_token(form, tag, lemma=None):
    token = MagicMock()
    token.form = form
    token.tag = tag
    token.lemma = lemma or form
    return token

"""
extract_all_keywords 실패: 카페 없음
"""
def test_extract_all_keywords_failure_no_cafes(mock_db_connection):
    mock_conn, mock_cursor = mock_db_connection
    # No cafes returned
    mock_cursor.fetchall.return_value = []
    # Run
    with patch.object(ke_module, "get_connection", return_value=mock_conn):
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        count = ke_module.extract_all_keywords()
    # Assertions
    assert count == 0
    # Ensure DELETE and SELECT id from cafes ran
    mock_cursor.execute.assert_any_call("DELETE FROM keywords")
    mock_cursor.execute.assert_any_call("ALTER TABLE keywords AUTO_INCREMENT = 1")
    mock_cursor.execute.assert_any_call("SELECT id FROM cafes")

"""
extract_all_keywords 성공: 리뷰가 있을 때 1개 키워드 반환
"""
def test_extract_all_keywords_success_with_reviews(mock_db_connection):
    mock_conn, mock_cursor = mock_db_connection
    # Setup cafes: one cafe with id 1
    mock_cursor.fetchall.side_effect = [
        [{"id": 1}],                   # for SELECT id FROM cafes
        [{"content": "테스트 리뷰"}],   # for SELECT content FROM reviews
    ]
    # Mock Kiwi.analyze to return a list of (list of tokens, score)
    mock_token1 = make_mock_token("맛있다", "VA", lemma="맛있다")
    mock_token2 = make_mock_token("커피", "NNG")
    fake_tokens = [[mock_token1, mock_token2]]
    with patch.object(ke_module, "Kiwi", return_value=MagicMock(analyze=lambda text: fake_tokens)):
        with patch.object(ke_module, "get_connection", return_value=mock_conn):
            mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
            count = ke_module.extract_all_keywords()
    # Assertions
    assert count == 1
    # Ensure keywords table reset
    mock_cursor.execute.assert_any_call("DELETE FROM keywords")
    # 적어도 DELETE, ALTER, SELECT id, SELECT content 외에 추가 쿼리가 실행되었는지 확인
    assert mock_cursor.execute.call_count > 3


"""
extract_all_keywords 실패: 형태소 분석 예외 처리
"""
def test_extract_all_keywords_failure_analysis_error(mock_db_connection):
    mock_conn, mock_cursor = mock_db_connection
    # 한 개의 카페 ID만 반환 및 리뷰 데이터도 반환
    mock_cursor.fetchall.side_effect = [
        [{"id": 42}],                # 첫 번째 fetchall: cafes 리스트
        [{"content": "테스트"}]      # 두 번째 fetchall: reviews 리스트
    ]
    # Patch Kiwi so that instantiation succeeds but analyze raises an exception
    with patch.object(ke_module, "Kiwi") as mock_Kiwi:
        mock_instance = MagicMock()
        mock_instance.analyze.side_effect = Exception("분석 실패")
        mock_Kiwi.return_value = mock_instance
        with patch.object(ke_module, "get_connection", return_value=mock_conn):
            mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
            # 함수가 예외를 다시 발생시키는지 확인합니다.
            with pytest.raises(Exception) as excinfo:
                ke_module.extract_all_keywords()
            assert "분석 실패" in str(excinfo.value)