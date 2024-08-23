from dagster import Config, op, OpExecutionContext, In, Nothing, Out
from pydantic import Field

from dagster_rudderstack.types import RudderStackProfilesOutput
from ..resources.rudderstack import RudderStackProfilesResource


class RudderStackProfilesOpConfig(Config):
    profile_id: str = Field(
        json_schema_extra={"is_required": True},
        description="The profileId for a profiles project.",
    )


@op(
    ins={"start_after": In(Nothing)},
    out=Out(RudderStackProfilesOutput, description="The output of the profile run."),
)
def rudderstack_profiles_op(
    context: OpExecutionContext,
    config: RudderStackProfilesOpConfig,
    profiles_resource: RudderStackProfilesResource,
):
    context.log.info("config_param: " + config.profile_id)
    output: RudderStackProfilesOutput = profiles_resource.start_and_poll(
        config.profile_id
    )
    return output
