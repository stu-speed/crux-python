"""
Module containing models that represent objects returned by the API.
"""

import logging
from logging import NullHandler

from crux.utils.client import Client


__all__ = (
    "CruxClient",
)

# Set default logging handler to avoid "No handler found" warnings
logging.getLogger(__name__).addHandler(NullHandler())
