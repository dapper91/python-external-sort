"""
External sort algorithm package.
"""

from .__about__ import (
    __title__,
    __description__,
    __url__,
    __version__,
    __author__,
    __email__,
    __license__
)
from .sort import sort
from .sort import Serializer
from .sort import Deserializer

__all__ = [
    '__title__',
    '__description__',
    '__url__',
    '__version__',
    '__author__',
    '__email__',
    '__license__',
    'sort',
    'Serializer',
    'Deserializer',
]
