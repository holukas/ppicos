"""Machine-specific path configuration for ppicos.

The source and output roots are site infrastructure paths and are kept out of
the code. They live in ``paths.toml`` at the repository root, which is
gitignored and never committed. Copy ``paths.example.toml`` to ``paths.toml``
and set the roots for your machine, or point the ``PPICOS_PATHS_FILE``
environment variable at a config file elsewhere.

The file is read once (cached) and only when a file type is actually built, so
``ppicos --list`` and ``ppicos --help`` work without a config present.
"""
import functools
import os
import tomllib
from pathlib import Path

_DEFAULT_CONFIG = Path(__file__).resolve().parent.parent / "paths.toml"


def config_path() -> Path:
    """Return the path to the active config file (env override wins)."""
    override = os.environ.get("PPICOS_PATHS_FILE")
    return Path(override) if override else _DEFAULT_CONFIG


@functools.lru_cache(maxsize=1)
def _load() -> dict:
    cfg_file = config_path()
    if not cfg_file.exists():
        raise FileNotFoundError(
            f"Path config not found: {cfg_file}\n"
            f"Copy 'paths.example.toml' to 'paths.toml' and set your roots, "
            f"or point PPICOS_PATHS_FILE at your config file."
        )
    with open(cfg_file, "rb") as f:
        return tomllib.load(f)


def _section(name: str, *keys: str) -> tuple[Path, ...]:
    cfg = _load()
    try:
        section = cfg[name]
        return tuple(Path(section[key]) for key in keys)
    except KeyError as e:
        raise KeyError(
            f"Missing key {e} under [{name}] in {config_path()}."
        ) from e


def roots() -> tuple[Path, Path]:
    """Return (rawdata_root, transfer_root) for the production data."""
    return _section("roots", "rawdata", "transfer")


def localtest_roots() -> tuple[Path, Path]:
    """Return (input_root, output_root) for local test data."""
    return _section("localtest", "input", "output")
