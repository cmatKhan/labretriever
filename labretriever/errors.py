"""Custom exception classes for dataset management."""


class HfDataFetchError(Exception):
    """Raised when HuggingFace API requests fail."""

    def __init__(
        self,
        message: str,
        repo_id: str | None = None,
        status_code: int | None = None,
        endpoint: str | None = None,
    ):
        super().__init__(message)
        self.repo_id = repo_id
        self.status_code = status_code
        self.endpoint = endpoint


class DataCardError(Exception):
    """Base exception for DataCard operations."""

    pass


class DataCardValidationError(DataCardError):
    """Exception raised when dataset card validation fails."""

    def __init__(
        self,
        message: str,
        repo_id: str | None = None,
        validation_errors: list | None = None,
    ):
        super().__init__(message)
        self.repo_id = repo_id
        self.validation_errors = validation_errors or []
