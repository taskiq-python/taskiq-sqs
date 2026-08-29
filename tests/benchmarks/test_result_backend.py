import pytest
from taskiq.result import TaskiqResult

from tests.benchmarks.conftest import RESULT_TASK_ID

from taskiq_sqs import S3ResultBackend


@pytest.mark.benchmark
async def test_set_result(s3_backend: S3ResultBackend[str], stored_result: TaskiqResult[str]) -> None:
    await s3_backend.set_result(RESULT_TASK_ID, stored_result)


@pytest.mark.benchmark
async def test_get_result(
    s3_backend: S3ResultBackend[str],
    stored_result: TaskiqResult[str],  # noqa: ARG001
) -> None:
    await s3_backend.get_result(RESULT_TASK_ID)


@pytest.mark.benchmark
async def test_is_result_ready(
    s3_backend: S3ResultBackend[str],
    stored_result: TaskiqResult[str],  # noqa: ARG001
) -> None:
    await s3_backend.is_result_ready(RESULT_TASK_ID)
