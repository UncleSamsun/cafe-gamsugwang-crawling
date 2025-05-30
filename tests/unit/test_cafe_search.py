import pytest
from unittest.mock import MagicMock
from app.service.cafe_search import search_cafes

"""
정상 호출 시 리스트 반환
"""
def test_search_cafes_success():
    # given
    mock_session = MagicMock()
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "documents": [
            {
                "id": "123",
                "place_name": "카페테스트",
                "x": "126.0",
                "y": "33.0"
            },
            {
                "id": "456",
                "place_name": "카페두번째",
                "x": "126.1",
                "y": "33.1"
            }
        ]
    }
    mock_session.get.return_value = mock_response

    # when
    result = search_cafes(
        min_lat="33.0",
        min_lng="126.0",
        max_lat="33.1",
        max_lng="126.1",
        api_key="FAKE_KEY",
        session=mock_session
    )

    # then
    assert isinstance(result, list)
    assert len(result) == 2
    assert result[0]["cafe_id"] == "123"
    assert result[1]["place_name"] == "카페두번째"

"""
documents 빈 배열 시 실패
"""
def test_search_cafes_failure_empty_docs():
    # given
    mock_session = MagicMock()
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"documents": []}
    mock_session.get.return_value = mock_response

    # when
    result = search_cafes(
        min_lat="33.0",
        min_lng="126.0",
        max_lat="33.1",
        max_lng="126.1",
        api_key="FAKE_KEY",
        session=mock_session
    )

    # then
    assert isinstance(result, list)
    assert len(result) == 0

"""
status_code != 200 시 실패
"""
def test_search_cafes_failure_status_code():
    # given
    mock_session = MagicMock()
    mock_response = MagicMock()
    mock_response.status_code = 500
    mock_response.json.return_value = {}
    mock_session.get.return_value = mock_response

    # when
    result = search_cafes(
        min_lat="33.0",
        min_lng="126.0",
        max_lat="33.1",
        max_lng="126.1",
        api_key="FAKE_KEY",
        session=mock_session
    )

    # then
    assert result == []