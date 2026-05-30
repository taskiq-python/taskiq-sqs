import asyncio
import logging
from collections import defaultdict
from collections.abc import AsyncGenerator, Callable, Mapping
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import boto3
from asyncer import asyncify
from botocore.exceptions import ClientError
from taskiq import AsyncBroker
from taskiq.abc.result_backend import AsyncResultBackend
from taskiq.acks import AckableMessage
from taskiq.message import BrokerMessage

from taskiq_sqs.aws import get_container_credentials
from taskiq_sqs.exceptions import BrokerInitError


if TYPE_CHECKING:
    from mypy_boto3_sqs.service_resource import Queue, SQSServiceResource


logger = logging.getLogger(__name__)


def stamp() -> int:  # noqa: D103
    return int(datetime.now(tz=timezone.utc).timestamp())


class SQSBroker(AsyncBroker):
    """AWS SQS TaskIQ broker."""

    def __init__(  # noqa: D107
        self,
        sqs_queue_url: str,
        wait_time_seconds: int = 0,  # Used for long polling
        max_number_of_messages: int = 1,  # size of batch to receive from the queue
        result_backend: AsyncResultBackend | None = None,
        task_id_generator: Callable[[], str] | None = None,
        sqs_region_override: str | None = None,
        force_ecs_container_credentials: bool = False,
    ) -> None:
        super().__init__(result_backend, task_id_generator)

        if not sqs_queue_url or not sqs_queue_url.startswith("http"):
            raise BrokerInitError(details="A valid SQS queue url is required")

        # NOTE: This bypasses the normal order of operations for boto3 auth and
        #       goes straight to using the ECS role creds from the metadata
        #       service. This can be useful in edge cases where there are higher
        #       priority credentials you do not want to use for this service.
        self.force_ecs_container_credentials = force_ecs_container_credentials
        self.sqs_region_override = sqs_region_override
        self.sqs_queue_url = sqs_queue_url
        self._sqs: SQSServiceResource | None = None
        self._sqs_queue: Queue | None = None
        self._creds_expiration: datetime | None = None

        if max_number_of_messages > 10:  # noqa: PLR2004
            raise BrokerInitError(details="MaxNumberOfMessages can be no greater than 10")

        self.wait_time_seconds = max(wait_time_seconds, 0)
        self.max_number_of_messages = max(max_number_of_messages, 1)

    @property
    def _sqs_credentials_expired(self) -> datetime | bool | None:
        return self._creds_expiration and self._creds_expiration < datetime.now(tz=timezone.utc)

    async def _sqs_client(self) -> "SQSServiceResource":
        if self._sqs and not self._sqs_credentials_expired:
            return self._sqs

        creds: Mapping[str, str] = defaultdict(None)

        if self.force_ecs_container_credentials:
            creds = await get_container_credentials()
            # NOTE: This is probably not an optional prop in the response
            if creds.get("Expiration"):
                self._creds_expiration = datetime.fromisoformat(creds["Expiration"])

        return boto3.resource(
            "sqs",
            region_name=self.sqs_region_override,
            aws_access_key_id=creds.get("AccessKeyId"),
            aws_secret_access_key=creds.get("SecretAccessKey"),
            aws_session_token=creds.get("Token"),
        )

    async def _get_queue(self) -> "Queue":
        if self._sqs_queue and not self._sqs_credentials_expired:
            return self._sqs_queue

        sqs = await self._sqs_client()
        self._sqs_queue = await asyncify(sqs.get_queue_by_name)(
            QueueName=self.sqs_queue_url.split("/")[-1],
        )

        if not self._sqs_queue:
            raise BrokerInitError(details="SQS queue not found")

        return self._sqs_queue

    async def kick(
        self,
        message: BrokerMessage,
    ) -> None:
        """This method is used to kick tasks out from current program.

        Using this method tasks are sent to
        workers.

        You don't need to send broker message. It's helper for brokers,
        please send only bytes from message.message.

        :param message: name of a task.
        """
        queue = await self._get_queue()
        # Must be explicitly set as a label to a unix timestamp
        expiry = message.labels.pop("sqs_expiry", 0)

        try:
            await asyncify(queue.send_message)(
                # SQS structured message attributes
                MessageAttributes={
                    "expiry": {
                        "StringValue": str(expiry),
                        "DataType": "Number",
                    },
                },
                MessageBody=message.message.decode("utf-8"),
                MessageGroupId=message.task_name,
            )
        except Exception:
            # taskiq suppresses the original exception, but it wold be good to know about
            logger.exception("Unhandled exception in SQSBroker")
            raise

    async def listen(self) -> AsyncGenerator[bytes | AckableMessage, None]:
        """This function listens to new messages and yields them.

        This it the main point for workers.
        This function is used to get new tasks from the network.

        If your broker support acknowledgement, then you
        should wrap your message in AckableMessage dataclass.

        If your messages was wrapped in AckableMessage dataclass,
        taskiq will call ack when finish processing message.

        :yield: incoming messages.
        :return: nothing.
        """
        # TODO: Consider using AckableMessage and confirm with the queue to reduce lost messages
        while True:
            no_backoff = False
            queue = await self._get_queue()

            try:
                for message in await asyncify(queue.receive_messages)(
                    MessageAttributeNames=[".*"],
                    # If there's competition on this queue (multiple processes of workers pulling
                    # from the same queue), and processing takes longer than the visibility timeout,
                    # multiple workers may end up processing the same message.
                    MaxNumberOfMessages=self.max_number_of_messages,
                    # Use long poling.
                    WaitTimeSeconds=self.wait_time_seconds,
                ):
                    try:
                        if message.message_attributes and (expiry_typed := message.message_attributes.get("expiry")):
                            expiry = int(expiry_typed.get("StringValue", 0))
                            now = stamp()
                            if 0 < expiry < now:
                                logger.warning(
                                    "Message expired %s seconds ago. Skipping.",
                                    now - expiry,
                                )
                                await asyncify(message.delete)()
                                no_backoff = True
                                continue
                    except TypeError:
                        # Ignore weird expiries.  Not critical.
                        pass

                    yield message.body.encode("utf-8")

                    try:
                        await asyncify(message.delete)()
                    except ClientError as err:
                        if "receipt handle has expired" in str(err):
                            # while not ideal, we shouldn't die on this
                            logger.exception(
                                "Message receipt handle has expired. This could indicate duplicate"
                                "processing or tasks being processed late.",
                            )
                        else:
                            raise

                    no_backoff = True
            except ClientError as err:
                # Creds will get refreshed when _get_queue() is called again
                if "ExpiredToken" in str(err):
                    logger.warning("ECS credentials expired.")
                    continue
                else:
                    raise

            sleepdur = 0.01 if no_backoff else 1
            logger.debug("No messages on queue. Broker is sleeping for %d seconds...", sleepdur)
            await asyncio.sleep(sleepdur)
            no_backoff = False
