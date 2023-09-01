from dataclasses import dataclass

@dataclass(frozen=True) # store constant
class ToGCSSetting:
    """ToGCS Settings."""
    source_path: str
    destination_path: str
    bucket_name: str