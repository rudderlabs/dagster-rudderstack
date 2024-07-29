import pytest
import requests
from unittest.mock import MagicMock, patch
from dagster_rudderstack.resources.rudderstack import (
    RudderStackRETLResource,
    RETLSyncStatus,
)
from dagster import Failure

from dagster_rudderstack.types import RudderStackRetlOutput


@pytest.fixture
def mock_retl_resource():
    return RudderStackRETLResource(
        access_token="test_access_token",
        rs_cloud_url="https://testapi.rudderstack.com",
        sync_poll_interval=1,
    )


def test_api_base_url(mock_retl_resource):
    assert mock_retl_resource.api_base_url == "https://testapi.rudderstack.com"


def test_request_headers(mock_retl_resource):
    headers = mock_retl_resource.request_headers
    assert headers["authorization"] == "Bearer test_access_token"
    assert headers["Content-Type"] == "application/json"


@patch("requests.request")
def test_make_request(mock_request, mock_retl_resource):
    mock_request.return_value = MagicMock(
        status_code=200, json=lambda: {"key": "value"}
    )

    response = mock_retl_resource.make_request(endpoint="/test-endpoint", method="GET")

    assert response == {"key": "value"}
    mock_request.assert_called_once()


@patch("requests.request")
def test_make_request_failure(mock_request, mock_retl_resource):
    mock_request.side_effect = requests.RequestException("Request failed")

    with pytest.raises(Failure, match="Exceeded max number of retries"):
        mock_retl_resource.make_request(endpoint="/test-endpoint", method="GET")


@patch("requests.request")
def test_start_sync(mock_request, mock_retl_resource):
    mock_request.return_value = MagicMock(
        status_code=200, json=lambda: {"syncId": "test_sync_id"}
    )

    sync_id = mock_retl_resource.start_sync(conn_id="test_conn_id")

    assert sync_id == "test_sync_id"
    mock_request.assert_called_once()


@patch("requests.request")
def test_poll_sync(mock_request, mock_retl_resource):
    mock_request.side_effect = [
        MagicMock(
            status_code=200,
            json=lambda: {"id": "test_sync_run_id", "status": RETLSyncStatus.RUNNING},
        ),
        MagicMock(
            status_code=200,
            json=lambda: {"id": "test_sync_run_id", "status": RETLSyncStatus.SUCCEEDED},
        ),
    ]

    result = mock_retl_resource.poll_sync(
        conn_id="test_conn_id", sync_id="test_sync_run_id"
    )

    assert mock_request.call_count == 2
    assert result == {"id": "test_sync_run_id", "status": RETLSyncStatus.SUCCEEDED}


@patch("requests.request")
def test_poll_sync_failure(mock_request, mock_retl_resource):
    mock_request.side_effect = [
        MagicMock(status_code=200, json=lambda: {"status": RETLSyncStatus.RUNNING}),
        MagicMock(
            status_code=200,
            json=lambda: {"status": RETLSyncStatus.FAILED, "error": "Test error"},
        ),
    ]

    with pytest.raises(
        Failure,
        match="Sync for retl connection: test_conn_id, syncId: test_sync_id failed with error: Test error",
    ):
        mock_retl_resource.poll_sync(conn_id="test_conn_id", sync_id="test_sync_id")


@patch("requests.request")
def test_start_and_poll(mock_request, mock_retl_resource):
    mock_request.side_effect = [
        MagicMock(status_code=200, json=lambda: {"syncId": "test_sync_run_id"}),
        MagicMock(
            status_code=200,
            json=lambda: {"id": "test_sync_run_id", "status": RETLSyncStatus.RUNNING},
        ),
        MagicMock(
            status_code=200,
            json=lambda: {"id": "test_sync_run_id", "status": RETLSyncStatus.SUCCEEDED},
        ),
    ]

    result = mock_retl_resource.start_and_poll(conn_id="test_conn_id")

    assert mock_request.call_count == 3
    assert result == RudderStackRetlOutput(
        {"id": "test_sync_run_id", "status": RETLSyncStatus.SUCCEEDED}
    )


if __name__ == "__main__":
    pytest.main()
