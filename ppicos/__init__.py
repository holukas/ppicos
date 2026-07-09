"""ppicos - Post-processing for ICOS flux tower data"""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("ppicos")
except PackageNotFoundError:
    # Package is not installed (e.g. running from source without `uv sync`)
    __version__ = "0.0.0+unknown"

from ppicos.main import IcosFormat
from ppicos.cli import main as cli_main

__all__ = ["IcosFormat", "cli_main"]
