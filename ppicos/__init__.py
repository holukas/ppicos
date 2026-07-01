"""ppicos - Post-processing for ICOS flux tower data"""

__version__ = "6.0.0"

from ppicos.main import IcosFormat
from ppicos.cli import main as cli_main

__all__ = ["IcosFormat", "cli_main"]
