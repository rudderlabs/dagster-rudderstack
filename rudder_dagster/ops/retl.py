from dagster import Config, op, OpExecutionContext
from pydantic import Field
from ..resources.rudderstack import RudderStackRETLResource


class RudderStackRETLOpConfig(Config):
    connection_id: str = Field(
        json_schema_extra={"is_required": True},
        description="The connectionId for an rETL sync.",
    )


@op
def rudderstack_sync_op(
    context: OpExecutionContext,
    config: RudderStackRETLOpConfig,
    retl_resource: RudderStackRETLResource,
):
    context.log.info("config_param: " + config.connection_id)
    retl_resource.start_and_poll(config.connection_id)
    return "Done"
