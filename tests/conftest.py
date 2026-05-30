from collections.abc import AsyncGenerator
from typing import TYPE_CHECKING, Any, TypedDict

import pytest
from aiobotocore.session import get_session

from taskiq_sqs import S3ResultBackend
from taskiq_sqs.bucket import S3Bucket


if TYPE_CHECKING:
    from types_aiobotocore_s3.client import S3Client

ENDPOINT_URL = "http://localhost:4566"
TEST_BUCKET = "test-bucket"


class AWSCredentials(TypedDict):
    endpoint_url: str
    aws_access_key_id: str
    aws_secret_access_key: str
    aws_region_name: str


@pytest.fixture(scope="session")
def aws_credentials() -> AWSCredentials:
    """Mocked AWS Credentials for moto."""
    return AWSCredentials(
        endpoint_url=ENDPOINT_URL,
        aws_access_key_id="your-aws-id",
        aws_secret_access_key="your-aws-access-key",  # noqa: S106  # pragma: allowlist secret
        aws_region_name="us-east-1",
    )


@pytest.fixture
async def s3_client(aws_credentials: AWSCredentials) -> "AsyncGenerator[S3Client, Any]":
    client_context = get_session().create_client(
        "s3",
        endpoint_url=aws_credentials["endpoint_url"],
        aws_access_key_id=aws_credentials["aws_access_key_id"],
        aws_secret_access_key=aws_credentials["aws_secret_access_key"],
        region_name=aws_credentials["aws_region_name"],
    )
    yield await client_context.__aenter__()
    await client_context.__aexit__(None, None, None)


@pytest.fixture
async def s3_bucket(s3_client: S3ResultBackend) -> AsyncGenerator[str, Any]:
    response = await s3_client.create_bucket(Bucket=TEST_BUCKET)
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
    # Ensure the bucket is created
    assert "Location" in response
    assert response["Location"] == f"/{TEST_BUCKET}"
    # Return the bucket name for use in tests
    yield TEST_BUCKET
    # Delete all objects in the bucket
    response = await s3_client.list_objects_v2(Bucket=TEST_BUCKET)
    if "Contents" in response:
        objects_to_delete = [{"Key": obj["Key"]} for obj in response.get("Contents", [])]
        if objects_to_delete:
            await s3_client.delete_objects(
                Bucket=TEST_BUCKET,
                Delete={"Objects": objects_to_delete},
            )

    # Delete the bucket itself
    await s3_client.delete_bucket(Bucket=TEST_BUCKET)


@pytest.fixture
async def s3_backend(
    aws_credentials: AWSCredentials,
    s3_bucket: str,  # noqa: ARG001
) -> AsyncGenerator[S3ResultBackend, Any]:
    backend = S3ResultBackend(bucket=S3Bucket(name=TEST_BUCKET), **aws_credentials)
    await backend.startup()
    assert backend._s3_client
    yield backend
    await backend.shutdown()
