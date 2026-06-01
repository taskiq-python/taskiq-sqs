from taskiq.exceptions import TaskiqError


class BaseTaskiqSQSError(TaskiqError):
    """Base error from taskiq-sqs."""


class BrokerInitError(BaseTaskiqSQSError):
    """Error during broker initialization."""

    __template__ = "Error during broker initialization: {details}"
    details: str


class InvalidEnvironmentError(BaseTaskiqSQSError):
    """Error in case something wrong with environment variables."""

    __template__ = "Something wrong with env: {details}"
    details: str


class ResultBackendError(BaseTaskiqSQSError):
    """Base error for all taskiq-aio-sqs broker exceptions."""

    __template__ = "Unexpected error occurred: {code}"
    code: str | None = None


class BucketNotFoundError(BaseTaskiqSQSError):
    """Error if bucket not found."""

    __template__ = "Bucket '{bucket_name}' not found during initialization and declare=False"
    bucket_name: str


class ResultIsMissingError(BaseTaskiqSQSError):
    """Error if there is no result when we trying to get it."""

    __template__ = "Result for task {task_id} is missing in the result backend"
    task_id: str
