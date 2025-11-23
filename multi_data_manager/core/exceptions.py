class MultiDataManagerError(Exception):
    """Base exception for multi_data_manager."""
    pass


class DatabaseError(MultiDataManagerError):
    """Raised when a database operation fails."""
    pass


class APIError(MultiDataManagerError):
    """Raised when an API call fails."""
    pass


class ConfigurationError(MultiDataManagerError):
    """Raised when there is a configuration issue."""
    pass
