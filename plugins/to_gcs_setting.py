from dataclasses import dataclass

@dataclass(frozen=True) # store constant
class ToGCSSetting:
    """ToGCS Settings."""
    src_name: str
    dir_name: str
    bucket_name: str