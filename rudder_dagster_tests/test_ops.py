import pytest
import responses
from unittest.mock import MagicMock
from dagster import build_op_context, ResourceDefinition
from dagster import job, RunConfig, Definitions
from rudder_dagster.ops.retl import rudderstack_sync_op, RudderStackRETLOpConfig
from rudder_dagster.resources.rudderstack import RudderStackRETLResource, RETLSyncStatus


@pytest.fixture
def mock_config():
    return RudderStackRETLOpConfig(connection_id="test_connection_id")


def test_rudderstack_sync_op(mock_config):
    context = build_op_context()
    retl_resource = MagicMock(spec=RudderStackRETLResource)
    ret_resource_def = ResourceDefinition.hardcoded_resource(retl_resource)
    result = rudderstack_sync_op(context, mock_config, ret_resource_def)
    retl_resource.start_and_poll.assert_called_with("test_connection_id")
    assert result == "Done"


def test_rudderstack_sync_job(mock_config):
    @job
    def rs_retl_test_sync_start_and_poll_job():
        rudderstack_sync_op()

    defs = Definitions(
        jobs=[rs_retl_test_sync_start_and_poll_job],
        resources={
            "retl_resource": RudderStackRETLResource(
                access_token="test_access_token",
                rs_cloud_url="https://testapi.rudderstack.com",
                sync_poll_interval=1,
            )
        },
    )

    with responses.RequestsMock() as rsps:
        rsps.add(
            rsps.POST,
            "https://testapi.rudderstack.com/v2/retl-connections/test_connection_id/start",
            json={"syncId": "test_sync_run_id"},
        )
        rsps.add(
            rsps.GET,
            "https://testapi.rudderstack.com/v2/retl-connections/test_connection_id/syncs/test_sync_run_id",
            json={"status": RETLSyncStatus.RUNNING},
        )
        rsps.add(
            rsps.GET,
            "https://testapi.rudderstack.com/v2/retl-connections/test_connection_id/syncs/test_sync_run_id",
            json={"status": RETLSyncStatus.SUCCEEDED},
        )
        job_result = defs.get_job_def(
            "rs_retl_test_sync_start_and_poll_job"
        ).execute_in_process(
            run_config=RunConfig(
                ops={
                    "rudderstack_sync_op": mock_config,
                }
            )
        )
        assert job_result.success


if __name__ == "__main__":
    pytest.main()
