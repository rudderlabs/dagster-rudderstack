## dagster_rudderstack

A Dagster library for triggering Reverse ETL syncs in RudderStack.

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
* request_max_retries: The maximum number of times requests to the RudderStack API should be retried before failng.
* request_retry_delay: Time (in seconds) to wait between each request retry.
* request_timeout: Time (in seconds) after which the requests to RudderStack are declared timed out.
* poll_interval: Time (in seconds) for polling status of triggered job.
* poll_timeout: Time (in seconds) after which the polling for a triggered job is declared timed out.
    
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

