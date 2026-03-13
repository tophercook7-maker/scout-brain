"""Scout run error types for structured error handling."""


class ScoutRunError(Exception):
    """Raised when scout run fails (API, SSL, geocode, etc)."""

    def __init__(self, error_type: str, error_message: str, user_friendly_message: str):
        super().__init__(error_message)
        self.error_type = error_type
        self.error_message = error_message
        self.user_friendly_message = user_friendly_message
