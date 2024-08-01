import pytest
import responses
from unittest.mock import MagicMock
from dagster import build_op_context, ResourceDefinition, op
from dagster import job, RunConfig, Definitions
from rudder_dagster.ops.retl import rudderstack_sync_op, RudderStackRETLOpConfig
from rudder_dagster.resources.rudderstack import RudderStackRETLResource, RETLSyncStatus
from rudder_dagster.types import RudderStackRetlOutput


@pytest.fixture
def mock_config():
    return RudderStackRETLOpConfig(connection_id="test_connection_id")


def test_rudderstack_sync_op(mock_config):
    context = build_op_context()
    retl_resource = MagicMock(spec=RudderStackRETLResource)
    retl_resource.start_and_poll.return_value = RudderStackRetlOutput(
        sync_run_details={"id": "test_sync_run_id", "status": RETLSyncStatus.SUCCEEDED}
    )
    ret_resource_def = ResourceDefinition.hardcoded_resource(retl_resource)
    result = rudderstack_sync_op(context, mock_config, ret_resource_def)
    retl_resource.start_and_poll.assert_called_with("test_connection_id")
    assert result == RudderStackRetlOutput(
        {"id": "test_sync_run_id", "status": RETLSyncStatus.SUCCEEDED}
    )


def test_rudderstack_sync_job(mock_config):
    @op
    def dummy_op():
        pass

    @job
    def rs_retl_test_sync_start_and_poll_job():
        rudderstack_sync_op(start_after=dummy_op())

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

    api_prefix = (
        "https://testapi.rudderstack.com/v2/retl-connections/test_connection_id/"
    )
    with responses.RequestsMock() as rsps:
        rsps.add(
            rsps.POST,
            api_prefix + "start",
            json={"syncId": "test_sync_run_id"},
        )
        rsps.add(
            rsps.GET,
            api_prefix + "syncs/test_sync_run_id",
            json={"id": "test_sync_run_id", "status": RETLSyncStatus.RUNNING},
        )
        rsps.add(
            rsps.GET,
            api_prefix + "syncs/test_sync_run_id",
            json={"id": "test_sync_run_id", "status": RETLSyncStatus.SUCCEEDED},
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
        assert job_result.output_for_node(
            "rudderstack_sync_op"
        ) == RudderStackRetlOutput(
            sync_run_details={
                "id": "test_sync_run_id",
                "status": RETLSyncStatus.SUCCEEDED,
            }
        )


if __name__ == "__main__":
    pytest.main()
