from dataclasses import dataclass


@dataclass
class S3Bucket:
    """
    Represents an S3 bucket configuration.

    Attributes:
        name: The name of the bucket.
        declare: Whether to create the bucket on startup if it not exists yet.
    """
    name: str
    declare: bool = True
