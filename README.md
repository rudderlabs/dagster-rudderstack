## dagster_rudderstack

A Dagster library for triggering Reverse ETL syncs and Profiles runs in RudderStack.

### Installation

Use pip to install the library.

```bash
pip install dagster_rudderstack
```

### Configuration
Setup RudderStack resource with your [workspace access token](https://www.rudderstack.com/docs/dashboard-guides/personal-access-token/).

```python
# resources.py
from dagster_rudderstack.resources.rudderstack import RudderStackRETLResource

rudderstack_retl_resource = RudderStackRETLResource(
            access_token="access_token")
```
RudderStackRETLResource exposes other configurable parameters as well. Mostly default values for them would be recommended.
* rs_cloud_url: RudderStack cloud endpoint.
* request_max_retries: The maximum number of times requests to the RudderStack API should be retried before failng.
* request_retry_delay: Time (in seconds) to wait between each request retry.
* request_timeout: Time (in seconds) after which the requests to RudderStack are declared timed out.
* poll_interval: Time (in seconds) for polling status of triggered job.
* poll_timeout: Time (in seconds) after which the polling for a triggered job is declared timed out.

Similarly if need to define ops and jobs for Profiles, can define a resource for profiles.
```python
# resources.py
from dagster_rudderstack.resources.rudderstack import RudderStackProfilesResource

rudderstack_profiles_resource = RudderStackProfilesResource(
            access_token="access_token")

```
    
### Ops and Jobs

Define ops and jobs with schedule. Provide the [connection id](https://www.rudderstack.com/docs/sources/reverse-etl/airflow-provider/#where-can-i-find-the-connection-id-for-my-reverse-etl-connection) for the sync job
```python
# jobs.py
from dagster import job, ScheduleDefinition, ScheduleDefinition
from dagster_rudderstack.ops.retl import rudderstack_sync_op, RudderStackRETLOpConfig
from .resources import rudderstack_retl_resource

@job(
    resource_defs={
        "retl_resource": rudderstack_retl_resource
    }
)
def rs_retl_sync_job():
        rudderstack_sync_op()

rudderstack_sync_schedule = ScheduleDefinition(
    job=rs_retl_sync_job,
    cron_schedule="* * * * *",  # Runs every minute
    run_config={"ops": {"rudderstack_sync_op": RudderStackRETLOpConfig(connection_id="connection_id")}},
    default_status=DefaultScheduleStatus.RUNNING
)
```

Similarly one can define ops for profiles job. Provide the [profiles id](https://www.rudderstack.com/docs/api/profiles-api/#run-project) for the profiles project to run.
```python
from dagster_rudderstack.ops.profiles import rudderstack_profiles_op, RudderStackProfilesOpConfig
from .resources import rudderstack_profiles_resource
@job(
    resource_defs={
        "profiles_resource": rudderstack_profiles_resource
    }
)
def rs_profiles_job():
        rudderstack_profiles_op()

```

RudderStackProfilesOpConfig also supports passing extra parameters that can be used for profile run via [profiles API](https://www.rudderstack.com/docs/api/profiles-api/)

In case, one wants to define a job as sequence of ops e.g, a profile run and then reverse etl sync run. Note that, if one of the op fails, job will raise exception without running the next op. One can configure job as needed. For example, can use try/catch exception to ignore op failure and still run second op.
```python
from dagster_rudderstack.ops.retl import rudderstack_sync_op, RudderStackRETLOpConfig
from dagster_rudderstack.ops.profiles import rudderstack_profiles_op, RudderStackProfilesOpConfig
from .resources import rudderstack_retl_resource, rudderstack_profiles_resource

@job(
    resource_defs={
        "profiles_resource": rudderstack_profiles_resource,
        "retl_resource": rudderstack_retl_resource
    }
)
def rs_profiles_then_retl_run():
    profiles_op = rudderstack_profiles_op()
    rudderstack_sync_op(start_after=profiles_op)

rudderstack_sync_schedule = ScheduleDefinition(
    job=rs_profiles_then_retl_run,
    cron_schedule="0 0 * * *",  # Runs day
    run_config=RunConfig(
                ops={
                    "rudderstack_profiles_op": RudderStackProfilesOpConfig(profile_id="profile_id", parameters=[<add list of optional parameters>]),
                    "rudderstack_sync_op": RudderStackRETLOpConfig(connection_id="connection_id"),
                }
        )    
    default_status=DefaultScheduleStatus.RUNNING
)
```