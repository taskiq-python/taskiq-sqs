import pytest

from tests.conftest import AWSCredentials

from taskiq_sqs import SQSBroker
from taskiq_sqs.exceptions import BrokerInitError


@pytest.mark.asyncio
async def test_get_queue_url_client_error(aws_credentials: AWSCredentials) -> None:
    broker = SQSBroker(queue_name="nonexistent-queue", **aws_credentials)
    with pytest.raises(BrokerInitError):
        await broker.startup()


@pytest.mark.asyncio
async def test_max_number_of_messages_error(aws_credentials: AWSCredentials) -> None:
    with pytest.raises(BrokerInitError):
        SQSBroker(
            queue_name="nonexistent-queue",
            max_number_of_messages=15,
            **aws_credentials,
        )
