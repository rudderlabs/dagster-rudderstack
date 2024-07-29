from dagster import Config, op, OpExecutionContext, In, Nothing, Out
from pydantic import Field

from dagster_rudderstack.types import RudderStackRetlOutput
from ..resources.rudderstack import RudderStackRETLResource


class RudderStackRETLOpConfig(Config):
    connection_id: str = Field(
        json_schema_extra={"is_required": True},
        description="The connectionId for an rETL sync.",
    )


@op(
    ins={"start_after": In(Nothing)},
    out=Out(RudderStackRetlOutput, description="The output of the sync run."),
)
def rudderstack_sync_op(
    context: OpExecutionContext,
    config: RudderStackRETLOpConfig,
    retl_resource: RudderStackRETLResource,
):
    context.log.info("config_param: " + config.connection_id)
    output: RudderStackRetlOutput = retl_resource.start_and_poll(config.connection_id)
    return output
