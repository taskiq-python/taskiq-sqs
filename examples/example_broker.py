"""
Run worker:
    taskiq worker examples.example_broker:broker

Run broker to send a task:
    python examples/example_broker.py
"""

import asyncio

import boto3
import dotenv
from taskiq_redis import RedisAsyncResultBackend

from taskiq_sqs import SQSBroker


dotenv.load_dotenv()

QUEUE_NAME = "my-queue"
QUEUE_URL = f"http://localhost:4566/000000000000/{QUEUE_NAME}"


boto3.client("sqs").create_queue(QueueName=QUEUE_NAME)

broker = SQSBroker(QUEUE_URL, sqs_region_override="us-east-1").with_result_backend(
    RedisAsyncResultBackend(redis_url="redis://localhost:6379")
)


@broker.task()
async def i_love_aws() -> None:
    """I hope my cloud bill doesn't get too high!"""
    await asyncio.sleep(5.5)
    print("Hello there!")


async def main() -> None:
    await broker.startup()
    task = await i_love_aws.kiq()
    print(await task.wait_result())
    await broker.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
