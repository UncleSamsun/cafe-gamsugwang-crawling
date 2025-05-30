from app.service.cafe_detail import get_connection
import pytest
from unittest.mock import patch, MagicMock
import app.service.cafe_detail as service_module

"""
crawl_all_cafes 성공 시 전체 카페 크롤링 결과 반환
"""
def test_crawl_all_cafes_success_default(mock_db_connection):
    # Setup DB mock
    mock_conn, mock_cursor = mock_db_connection

    mock_cafe_ids = ["cafe123", "cafe456"]
    mock_cursor.fetchall.return_value = [{"id": cid} for cid in mock_cafe_ids]

    with patch.object(service_module, "get_connection", return_value=mock_conn):
        mock_conn.cursor.return_value = mock_cursor
        with patch.object(service_module, "crawl_and_save_single_cafe", return_value=True) as mock_crawl_single:
            result = service_module.crawl_all_cafes()
            for cid in mock_cafe_ids:
                mock_crawl_single.assert_any_call(cid)

    # Assert
    assert isinstance(result, dict)
    assert result["crawled_cafes"] == len(mock_cafe_ids)
    assert result["failed_ids"] == []
    assert mock_cursor.execute.called


"""
crawl_all_cafes 재시도 로직으로 실패한 ID 재처리 후 완료
"""
def test_crawl_all_cafes_failure_with_retry(mock_db_connection):
    # Setup DB mock
    mock_conn, mock_cursor = mock_db_connection
    mock_cafe_ids = ["cafeA", "cafeB", "cafeC"]
    mock_cursor.fetchall.return_value = [{"id": cid} for cid in mock_cafe_ids]

    # Patch get_connection to return mock connection
    with patch.object(service_module, "get_connection", return_value=mock_conn):
        mock_conn.cursor.return_value = mock_cursor

        # crawl_and_save_single_cafe: 첫 두 개는 True, 마지막은 False, 재시도는 True로 모킹
        with patch.object(
            service_module,
            "crawl_and_save_single_cafe",
            side_effect=[True, True, False, True]
        ) as mock_crawl_single:
            result = service_module.crawl_all_cafes()

            # 모든 ID로 호출이 발생했는지 검증
            for cid in mock_cafe_ids:
                mock_crawl_single.assert_any_call(cid)

    # 반환값 검증
    assert isinstance(result, dict)
    assert result["crawled_cafes"] == 3        # 모든 시도까지 포함하여 성공 3개
    assert result["failed_ids"] == []         # 재시도 성공으로 실패 목록 없음

    # DB 쿼리 호출 여부 확인
    assert mock_cursor.execute.called
