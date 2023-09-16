from dataclasses import dataclass

@dataclass(frozen=True) # store constant
class GCPConnectionSettings:
    """Connection Settings."""
    conn_id: str
    conn_type: str
    scope: str
    project: str
    bucket_keypath: str
    bq_keypath: str