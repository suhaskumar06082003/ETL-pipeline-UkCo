"""Domain exceptions for the ETL pipeline.

Framework-agnostic so the pipeline stages (extract/transform/load) can be
reused from the API and from Airflow without importing FastAPI.
"""


class ETLError(Exception):
    """Base class for all ETL errors."""


class ValidationError(ETLError):
    """Raised when a file or its columns fail validation."""


class ConfigurationError(ETLError):
    """Raised when a required credential / setting is missing."""


class ExtractError(ETLError):
    """Raised when a file cannot be read/parsed."""


class LoadError(ETLError):
    """Raised when data cannot be written to a destination (blob / MySQL)."""
