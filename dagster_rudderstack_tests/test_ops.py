import pytest
import responses
import datetime
from unittest.mock import MagicMock
from dagster import Failure, build_op_context, ResourceDefinition, op
from dagster import job, RunConfig, Definitions
from dagster import (
    ScheduleEvaluationContext,
    RunRequest,
    schedule,
    build_schedule_context,
    validate_run_config,
)
from dagster_rudderstack.ops.retl import rudderstack_sync_op, RudderStackRETLOpConfig
from dagster_rudderstack.ops.profiles import (
    rudderstack_profiles_op,
    RudderStackProfilesOpConfig,
)
from dagster_rudderstack.resources.rudderstack import (
    RudderStackRETLResource,
    RETLSyncStatus,
    RudderStackProfilesResource,
    ProfilesRunStatus,
)
from dagster_rudderstack.types import RudderStackRetlOutput, RudderStackProfilesOutput


@pytest.fixture
def mock_retl_op_config():
    return RudderStackRETLOpConfig(connection_id="test_connection_id")


@pytest.fixture
def mock_profiles_op_config():
    return RudderStackProfilesOpConfig(profile_id="test_profile_id")


@pytest.fixture
def mock_retl_resource():
    return RudderStackRETLResource(
        access_token="test_access_token",
        rs_cloud_url="https://testapi.rudderstack.com",
        poll_interval=0.1,
    )


@pytest.fixture
def mock_profiles_resource():
    return RudderStackProfilesResource(
        access_token="test_access_token",
        rs_cloud_url="https://testapi.rudderstack.com",
        poll_interval=0.1,
    )


def test_rudderstack_sync_op(mock_retl_op_config):
    context = build_op_context()
    retl_resource = MagicMock(spec=RudderStackRETLResource)
    retl_resource.start_and_poll.return_value = RudderStackRetlOutput(
        sync_run_details={"id": "test_sync_run_id", "status": RETLSyncStatus.SUCCEEDED}
    )
    ret_resource_def = ResourceDefinition.hardcoded_resource(retl_resource)
    result = rudderstack_sync_op(context, mock_retl_op_config, ret_resource_def)
    retl_resource.start_and_poll.assert_called_with("test_connection_id")
    assert result == RudderStackRetlOutput(
        {"id": "test_sync_run_id", "status": RETLSyncStatus.SUCCEEDED}
    )


def test_rudderstack_profiles_op(mock_profiles_op_config):
    context = build_op_context()
    profiles_resource = MagicMock(spec=RudderStackProfilesResource)
    profiles_resource.start_and_poll.return_value = RudderStackProfilesOutput(
        profiles_run_details={
            "id": "test_profiles_run_id",
            "status": ProfilesRunStatus.FINISHED,
        }
    )
    profiles_resource_def = ResourceDefinition.hardcoded_resource(profiles_resource)
    result = rudderstack_profiles_op(
        context, mock_profiles_op_config, profiles_resource_def
    )
    profiles_resource.start_and_poll.assert_called_with("test_profile_id", None)
    assert result == RudderStackProfilesOutput(
        {"id": "test_profiles_run_id", "status": ProfilesRunStatus.FINISHED}
    )


def test_rudderstack_sync_job(mock_retl_op_config, mock_retl_resource):
    @op
    def dummy_op():
        pass

    @job
    def rs_retl_test_sync_start_and_poll_job():
        rudderstack_sync_op(start_after=dummy_op())

    defs = Definitions(
        jobs=[rs_retl_test_sync_start_and_poll_job],
        resources={"retl_resource": mock_retl_resource},
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
                    "rudderstack_sync_op": mock_retl_op_config,
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


def test_rudderstack_sync_job_timeout(mock_retl_op_config):
    @job
    def rs_retl_test_sync_start_and_poll_job():
        rudderstack_sync_op()

    defs = Definitions(
        jobs=[rs_retl_test_sync_start_and_poll_job],
        resources={
            "retl_resource": RudderStackRETLResource(
                access_token="test_access_token",
                rs_cloud_url="https://testapi.rudderstack.com",
                poll_interval=0.1,
                poll_timeout=0.2,
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
            json={"id": "test_sync_run_id", "status": RETLSyncStatus.RUNNING},
        )
        rsps.add(
            rsps.GET,
            api_prefix + "syncs/test_sync_run_id",
            json={"id": "test_sync_run_id", "status": RETLSyncStatus.RUNNING},
        )
        with pytest.raises(Failure):
            defs.get_job_def("rs_retl_test_sync_start_and_poll_job").execute_in_process(
                run_config=RunConfig(
                    ops={
                        "rudderstack_sync_op": mock_retl_op_config,
                    }
                )
            )


def test_profiles_run_job(mock_profiles_op_config, mock_profiles_resource):
    @op
    def dummy_op():
        pass

    @job
    def rs_profiles_test_start_and_poll_job():
        rudderstack_profiles_op(start_after=dummy_op())

    defs = Definitions(
        jobs=[rs_profiles_test_start_and_poll_job],
        resources={"profiles_resource": mock_profiles_resource},
    )

    api_prefix = "https://testapi.rudderstack.com/v2/sources/test_profile_id/"
    with responses.RequestsMock() as rsps:
        rsps.add(
            rsps.POST,
            api_prefix + "start",
            json={"runId": "test_profiles_run_id"},
        )
        rsps.add(
            rsps.GET,
            api_prefix + "runs/test_profiles_run_id/status",
            json={"id": "test_profiles_run_id", "status": ProfilesRunStatus.RUNNING},
        )
        rsps.add(
            rsps.GET,
            api_prefix + "runs/test_profiles_run_id/status",
            json={"id": "test_profiles_run_id", "status": ProfilesRunStatus.FINISHED},
        )
        job_result = defs.get_job_def(
            "rs_profiles_test_start_and_poll_job"
        ).execute_in_process(
            run_config=RunConfig(
                ops={
                    "rudderstack_profiles_op": mock_profiles_op_config,
                }
            )
        )
        assert job_result.output_for_node(
            "rudderstack_profiles_op"
        ) == RudderStackProfilesOutput(
            profiles_run_details={
                "id": "test_profiles_run_id",
                "status": ProfilesRunStatus.FINISHED,
            }
        )


def test_profiles_run_with_params(mock_profiles_resource):
    profiles_op_config = RudderStackProfilesOpConfig(
        profile_id="test_profile_id", parameters=["--rebase_incremental"]
    )

    @op
    def dummy_op():
        pass

    @job
    def rs_profiles_test_start_and_poll_job():
        rudderstack_profiles_op(start_after=dummy_op())

    defs = Definitions(
        jobs=[rs_profiles_test_start_and_poll_job],
        resources={"profiles_resource": mock_profiles_resource},
    )

    api_prefix = "https://testapi.rudderstack.com/v2/sources/test_profile_id/"
    with responses.RequestsMock() as rsps:
        rsps.add(
            rsps.POST,
            api_prefix + "start",
            json={"runId": "test_profiles_run_id"},
            match=[
                responses.json_params_matcher({"parameters": ["--rebase_incremental"]})
            ],
        )
        rsps.add(
            rsps.GET,
            api_prefix + "runs/test_profiles_run_id/status",
            json={"id": "test_profiles_run_id", "status": ProfilesRunStatus.RUNNING},
        )
        rsps.add(
            rsps.GET,
            api_prefix + "runs/test_profiles_run_id/status",
            json={"id": "test_profiles_run_id", "status": ProfilesRunStatus.FINISHED},
        )
        job_result = defs.get_job_def(
            "rs_profiles_test_start_and_poll_job"
        ).execute_in_process(
            run_config=RunConfig(
                ops={
                    "rudderstack_profiles_op": profiles_op_config,
                }
            )
        )
        assert job_result.output_for_node(
            "rudderstack_profiles_op"
        ) == RudderStackProfilesOutput(
            profiles_run_details={
                "id": "test_profiles_run_id",
                "status": ProfilesRunStatus.FINISHED,
            }
        )


def test_profiles_retl_run(
    mock_profiles_op_config,
    mock_retl_op_config,
    mock_profiles_resource,
    mock_retl_resource,
):
    @job
    def rs_profiles_then_retl_run():
        profiles_op = rudderstack_profiles_op()
        rudderstack_sync_op(start_after=profiles_op)

    defs = Definitions(
        jobs=[rs_profiles_then_retl_run],
        resources={
            "retl_resource": mock_retl_resource,
            "profiles_resource": mock_profiles_resource,
        },
    )
    retl_api_prefix = (
        "https://testapi.rudderstack.com/v2/retl-connections/test_connection_id/"
    )
    profiles_api_prefix = "https://testapi.rudderstack.com/v2/sources/test_profile_id/"

    with responses.RequestsMock() as rsps:
        rsps.add(
            rsps.POST,
            profiles_api_prefix + "start",
            json={"runId": "test_profiles_run_id"},
        )
        rsps.add(
            rsps.GET,
            profiles_api_prefix + "runs/test_profiles_run_id/status",
            json={"id": "test_profiles_run_id", "status": ProfilesRunStatus.RUNNING},
        )
        rsps.add(
            rsps.GET,
            profiles_api_prefix + "runs/test_profiles_run_id/status",
            json={"id": "test_profiles_run_id", "status": ProfilesRunStatus.FINISHED},
        )
        rsps.add(
            rsps.POST,
            retl_api_prefix + "start",
            json={"syncId": "test_sync_run_id"},
        )
        rsps.add(
            rsps.GET,
            retl_api_prefix + "syncs/test_sync_run_id",
            json={"id": "test_sync_run_id", "status": RETLSyncStatus.RUNNING},
        )
        rsps.add(
            rsps.GET,
            retl_api_prefix + "syncs/test_sync_run_id",
            json={"id": "test_sync_run_id", "status": RETLSyncStatus.SUCCEEDED},
        )
        job_result = defs.get_job_def("rs_profiles_then_retl_run").execute_in_process(
            run_config=RunConfig(
                ops={
                    "rudderstack_profiles_op": mock_profiles_op_config,
                    "rudderstack_sync_op": mock_retl_op_config,
                }
            )
        )
        assert job_result.output_for_node(
            "rudderstack_profiles_op"
        ) == RudderStackProfilesOutput(
            profiles_run_details={
                "id": "test_profiles_run_id",
                "status": ProfilesRunStatus.FINISHED,
            }
        )
        assert job_result.output_for_node(
            "rudderstack_sync_op"
        ) == RudderStackRetlOutput(
            sync_run_details={
                "id": "test_sync_run_id",
                "status": RETLSyncStatus.SUCCEEDED,
            }
        )


def test_scheduled_profile_retl_run(
    mock_profiles_op_config,
    mock_retl_op_config,
    mock_profiles_resource,
    mock_retl_resource,
):
    @job
    def rs_profiles_then_retl():
        profiles_op = rudderstack_profiles_op()
        rudderstack_sync_op(start_after=profiles_op)

    @schedule(job=rs_profiles_then_retl, cron_schedule="0 0 * * *")
    def rs_profiles_then_retl_job_schedule(context: ScheduleEvaluationContext):
        scheduled_date = context.scheduled_execution_time.strftime("%Y-%m-%d")
        return RunRequest(
            run_key=None,
            run_config=RunConfig(
                ops={
                    "rudderstack_profiles_op": mock_profiles_op_config,
                    "rudderstack_sync_op": mock_retl_op_config,
                }
            ),
            tags={"date": scheduled_date},
        )

    Definitions(
        jobs=[rs_profiles_then_retl],
        resources={
            "retl_resource": mock_retl_resource,
            "profiles_resource": mock_profiles_resource,
        },
        schedules=[rs_profiles_then_retl_job_schedule],
    )

    context = build_schedule_context(
        scheduled_execution_time=datetime.datetime(2020, 1, 1)
    )
    run_request = rs_profiles_then_retl_job_schedule(context)
    assert validate_run_config(rs_profiles_then_retl, run_request.run_config)
    assert run_request.tags == {"date": "2020-01-01"}


if __name__ == "__main__":
    pytest.main()
