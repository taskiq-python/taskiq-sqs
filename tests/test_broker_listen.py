from taskiq_sqs import SQSBroker


async def test_when_listen__than_we_should_delete_message_from_queue(
    sqs_broker: SQSBroker, sqs_queue: str,
) -> None:
    await sqs_broker._sqs_client.send_message(
        QueueUrl=sqs_queue,
        MessageBody="test_message",
    )

    messages = []
    async for message in sqs_broker.listen():
        messages.append(message)
        await message.ack()
        break

    assert len(messages) == 1
    assert messages[0].data == b"test_message"

    response = await sqs_broker._sqs_client.receive_message(QueueUrl=sqs_queue)
    assert "Messages" not in response
