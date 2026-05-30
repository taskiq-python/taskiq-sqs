from unittest.mock import AsyncMock

import pytest
from taskiq.result import TaskiqResult

from taskiq_sqs import S3ResultBackend
from taskiq_sqs.exceptions import ResultIsMissingError


@pytest.fixture
def taskiq_result() -> TaskiqResult:
    return TaskiqResult(return_value="test_value", is_err=True, execution_time=0.1)


class TestResultBackend:
    async def test_when_result_set__then_result_is_actually_saved_to_s3(
        self,
        s3_backend: S3ResultBackend,
        s3_bucket: str,
        taskiq_result: TaskiqResult,
    ) -> None:
        await s3_backend.set_result("test_task_id", taskiq_result)

        response = await s3_backend._s3_client.get_object(
            Bucket=s3_bucket,
            Key="test_task_id",
        )
        assert response["Body"] is not None

    async def test_when_result_present_in_s3__then_get_result_return_it(
        self,
        s3_backend: S3ResultBackend,
        taskiq_result: TaskiqResult,
    ) -> None:
        await s3_backend.set_result("test_task_id", taskiq_result)

        retrieved_result = await s3_backend.get_result("test_task_id")
        assert retrieved_result.return_value == "test_value"
        assert retrieved_result.is_err is True

    async def test_when_result_is_missing__then_get_result_raise_exception(
        self,
        s3_backend: S3ResultBackend,
    ) -> None:
        s3_backend._s3_client.get_object = AsyncMock(return_value={})  # Simulate a response with no Body
        with pytest.raises(ResultIsMissingError):
            await s3_backend.get_result("test_task_id")

    async def test_when_set_result_is_called__then_save_it_to_right_path(
        self,
        s3_backend: S3ResultBackend,
        s3_bucket: str,
        taskiq_result: TaskiqResult,
    ) -> None:
        s3_backend._base_path = "results"
        await s3_backend.set_result("test_task_id", taskiq_result)

        response = await s3_backend._s3_client.head_object(
            Bucket=s3_bucket,
            Key="results/test_task_id",
        )
        assert response is not None

    async def test_when_result_is_set__then_we_should_be_able_to_get_it(
        self,
        s3_backend: S3ResultBackend,
        taskiq_result: TaskiqResult,
    ) -> None:
        s3_backend._base_path = "results"
        await s3_backend.set_result("test_task_id", taskiq_result)

        retrieved_result = await s3_backend.get_result("test_task_id")
        assert retrieved_result.return_value == "test_value"
        assert retrieved_result.is_err is True

    async def test_when_result_is_missing__then_get_result_should_raise_an_error(
        self,
        s3_backend: S3ResultBackend,
    ) -> None:
        with pytest.raises(ResultIsMissingError):
            await s3_backend.get_result("non_existent_task_id")

    async def test_when_result_exists__when_is_result_ready_should_return_true(
        self,
        s3_backend: S3ResultBackend,
        taskiq_result: TaskiqResult,
    ) -> None:
        s3_backend._base_path = "results"
        await s3_backend.set_result("test_task_id", taskiq_result)

        assert await s3_backend.is_result_ready("test_task_id") is True
        assert await s3_backend.is_result_ready("non_existent_task_id") is False

    async def test_when_get_result_without_logs__then_we_should_return_none(self, s3_backend: S3ResultBackend) -> None:
        result = TaskiqResult(
            return_value="test_value",
            log="test_log",
            is_err=False,
            execution_time=0.1,
        )
        await s3_backend.set_result("test_task_id", result)

        retrieved_result = await s3_backend.get_result("test_task_id", with_logs=False)
        assert retrieved_result.return_value == "test_value"
        assert retrieved_result.is_err is False
        assert retrieved_result.log is None

    async def test_when_get_result_with_logs__then_we_should_return_them(self, s3_backend: S3ResultBackend) -> None:
        result = TaskiqResult(
            return_value="test_value",
            is_err=True,
            log="test_log",
            execution_time=0.1,
        )
        await s3_backend.set_result("test_task_id", result)

        retrieved_result = await s3_backend.get_result("test_task_id", with_logs=True)
        assert retrieved_result.return_value == "test_value"
        assert retrieved_result.is_err is True
        assert retrieved_result.log == "test_log"
