import pytest
from taskiq import BrokerMessage

from taskiq_sqs import SQSBroker
from taskiq_sqs.exceptions import BrokerInitError


async def test_when_kick_called__than_message_should_be_published_to_queue(
    sqs_broker: SQSBroker,
    sqs_queue: str,
    broker_message: BrokerMessage,
) -> None:
    await sqs_broker.kick(broker_message)
    response = await sqs_broker._sqs_client.receive_message(QueueUrl=sqs_queue)
    assert "Messages" in response
    assert len(response["Messages"]) == 1
    assert response["Messages"][0]["Body"] == "test_message"


async def test_when_during_kick_queue_not_found__then_should_raise_an_error(
    sqs_broker: SQSBroker,
    broker_message: BrokerMessage,
) -> None:
    sqs_broker._sqs_queue_url = "nonexistent-queue"
    with pytest.raises(BrokerInitError):
        await sqs_broker.kick(broker_message)
