from dataclasses import dataclass

@dataclass(frozen=True) # store constant
class ToGCSSetting:
    """ToGCS Settings."""
    dir_name: str
    bucket_name: str