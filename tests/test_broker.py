from taskiq_sqs import SQSBroker


class TestInitParameters:
    async def test_initialization_logic(self) -> None:
        broker = SQSBroker("http://localhost:4566/000000000000/my-queue")
        assert broker.sqs_queue_url == "http://localhost:4566/000000000000/my-queue"
        assert broker.force_ecs_container_credentials is False
        assert broker.sqs_region_override is None
        assert broker._sqs_queue is None
