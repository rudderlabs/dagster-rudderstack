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
