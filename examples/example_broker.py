"""
Run worker:
    taskiq worker examples.example_broker:broker

Run broker to send a task:
    python examples/example_broker.py
"""

import asyncio

import boto3
import dotenv

from taskiq_sqs import S3Bucket, S3ResultBackend, SQSBroker


dotenv.load_dotenv()

QUEUE_NAME = "my-queue"
QUEUE_URL = f"http://localhost:4566/000000000000/{QUEUE_NAME}"


boto3.client("sqs").create_queue(QueueName=QUEUE_NAME)

broker = SQSBroker(QUEUE_URL, sqs_region_override="us-east-1").with_result_backend(
    S3ResultBackend(bucket=S3Bucket(name="response-bucket"))
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
