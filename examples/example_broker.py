"""
Run worker:
    taskiq worker examples.example_broker:broker

Run broker to send a task:
    python examples/example_broker.py
"""

import asyncio

import dotenv
from aiobotocore.session import get_session

from taskiq_sqs import S3Bucket, S3ResultBackend, SQSBroker


dotenv.load_dotenv()

QUEUE_NAME = "my-queue"
ENDPOINT_URL = "http://localhost:4566"
AWS_REGION = "us-east-1"


broker = SQSBroker(
    queue_name=QUEUE_NAME,
    endpoint_url=ENDPOINT_URL,
    aws_region_name=AWS_REGION,
).with_result_backend(
    S3ResultBackend(
        bucket=S3Bucket(name="response-bucket"),
        endpoint_url=ENDPOINT_URL,
        aws_region_name=AWS_REGION,
    ),
)


@broker.task()
async def i_love_aws() -> None:
    """I hope my cloud bill doesn't get too high!"""
    await asyncio.sleep(2)
    print("Hello there!")


async def ensure_queue_exists() -> None:
    session = get_session()
    async with session.create_client(
        "sqs",
        region_name=AWS_REGION,
        endpoint_url=ENDPOINT_URL,
    ) as sqs:
        await sqs.create_queue(QueueName=QUEUE_NAME)


async def main() -> None:
    await ensure_queue_exists()
    await broker.startup()
    task = await i_love_aws.kiq()
    print(await task.wait_result())
    await broker.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
