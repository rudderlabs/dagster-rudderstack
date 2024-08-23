from typing import Any, Dict, NamedTuple


class RudderStackRetlOutput(
    NamedTuple(
        "_RudderStackRetlOutput",
        [
            ("sync_run_details", Dict[str, Any]),
        ],
    )
):
    """Contains details of a sync run.

    Attributes:
        sync_run_details (Dict[str, Any]): Details of the sync run.
        {
            "id": "string",
            "status": "running",
            "startedAt": "2024-08-01T06:14:33.744Z",
            "finishedAt": "2024-08-01T06:14:33.744Z",
            "error": "string",
            "metrics": {
                "succeeded": {
                    "total": 0
                },
                "failed": {
                    "total": 0
                },
                "changed": {
                    "total": 0
                },
            "total": 0
            }
        }
    """


class RudderStackProfilesOutput(
    NamedTuple(
        "_RudderStackProfilesOutput",
        [
            ("profiles_run_details", Dict[str, Any]),
        ],
    )
):
    """Contains details of a profiles run.

    Attributes:
        profiles_run_details (Dict[str, Any]): Details of the profiles run.
        {
            "id": "cqf7pvv8sp57cfhreg0g",
            "job_id": "2hkeBOp0lGJpMPyur0oXUxxIDhw",
            "started_at": "2024-07-22T15:38:39.059503Z",
            "finished_at": "2024-07-22T15:42:38.297066Z",
            "status": "finished",
            "tasks": [
                {
                    "task_run_id": "cqf7pvv8sp57cfhreg10",
                    "task_id": "wht",
                    "job_id": "2hkeBOp0lGJpMPyur0oXUxxIDhw",
                    "started_at": "2024-07-22T15:38:39.218306Z",
                    "finished_at": "2024-07-22T15:42:38.010303Z"
                }
            ]
        }
    """
