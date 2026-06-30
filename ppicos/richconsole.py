"""Shared Rich console, theme, and presentation layer for ppicos.

This module is the single source of truth for how ppicos renders to the
terminal. The Logger (per-line operation output) and the CLI (banners,
summaries) import from here so colors and styling stay consistent.

Design contract (read before changing console output anywhere):

* Console output is decorative only. The plain-text LOG FILE always receives
  the full, original messages (written elsewhere via the logging module).
  The console may *shorten* a line (e.g. trim absolute paths to a basename,
  drop redundant divider rows) but must not drop a meaningful event.

* Markup is DISABLED for message text. Existing messages contain bracket
  tokens like ``[exporting daily files]`` and ``[1, 2]`` that Rich would
  otherwise parse as style markup, so all rendering uses ``rich.text.Text``
  (never inline ``[style]`` markup) for untrusted message text.

* Modern truecolor palette. Styles are semantic names, retunable in one place.
"""
import re
import sys

from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from rich.theme import Theme

# Ensure the modern glyphs (✓ → • … etc.) can always be written, even when
# stdout/stderr are redirected to a non-UTF-8 stream (Windows pipes, cmd).
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, ValueError, OSError):
        pass

# Modern truecolor palette (semantic style names).
THEME = Theme({
    "info": "grey85",           # normal operation lines
    "muted": "grey50",          # secondary / timing / dividers
    "accent": "#a78bfa",        # violet, for emphasis / actions
    "heading": "bold #a78bfa",
    "section": "bold #22d3ee",   # cyan section rules
    "success": "bold #34d399",   # emerald
    "warning": "bold #fbbf24",   # amber
    "error": "bold #f87171",     # red
    "skip": "#fb923c",           # orange, for skipped / already-processed items
    "path": "#60a5fa",           # blue, for file names
})

# Shared console. highlight=False keeps log lines calm (no auto recoloring of
# every number/path); structure comes from explicit styles, rules and panels.
console = Console(theme=THEME, highlight=False)

# Matches an absolute path: Windows drive (C:\ or C:/), UNC (\\host) or //host.
_PATH_RE = re.compile(r"(?:[A-Za-z]:[\\/]|\\\\|//)[^\s,;()]+")
# Matches a leading section tag like "[exporting daily files] ".
_SECTION_RE = re.compile(r"^\s*\[[^\]]+\]\s?")


def _short_path(path: str) -> str:
    """Shorten an absolute path for the console: file -> basename,
    directory -> '…/<last two components>'. Full path stays in the log file."""
    parts = [p for p in re.split(r"[\\/]+", path.strip("\\/")) if p]
    if len(parts) <= 1:
        return path
    tail = parts[-1]
    if "." in tail:  # looks like a file name
        return tail
    return "…/" + "/".join(parts[-2:])


def _shorten_paths(text: str) -> str:
    return _PATH_RE.sub(lambda m: _short_path(m.group(0)), text)


def style_for_line(text: str) -> str:
    """Pick a semantic style for a raw operation log line from its markers."""
    lowered = text.lower()
    if "(!)" in text or "warning" in lowered or "skipping" in lowered:
        return "warning"
    if "no files found" in lowered or "stopping script" in lowered:
        return "error"
    if "script runtime" in lowered:
        return "success"
    return "info"


def _format_physical_line(line: str):
    """Transform one physical log line into a styled Text, or None to drop it."""
    # Drop the section-tag prefix; the section rule already shows the phase.
    m = _SECTION_RE.match(line)
    body = line[m.end():] if m else line

    indent_spaces = len(body) - len(body.lstrip(" "))
    content = body.strip()

    if not content:
        return None
    if set(content) <= set("-= "):           # pure divider row
        return None
    if "SECTION START" in content:            # redundant with the rule
        return None

    style = style_for_line(line)
    glyph = ""

    if content.startswith("--> "):
        glyph, content, style = "→ ", content[4:], "accent"
    elif content.startswith("--| "):
        glyph, content, style = "⊘ ", content[4:], "skip"
    elif content.startswith("++ "):
        rest = content[3:].replace("ADDING FILE", "")
        rest = re.sub(r"\s*-\s*for further processing\s*$", "", rest).strip()
        glyph, content, style = "+ ", f"added {rest}", "success"
    elif content.startswith("* "):
        glyph, content = "• ", content[2:]
    elif content.startswith("(i) "):
        glyph, content, style = "ℹ ", content[4:], "info"
    elif content.startswith("(!) "):
        glyph, content, style = "⚠ ", content[4:], "warning"

    content = _shorten_paths(content)
    indent = "  " * min(indent_spaces // 4, 4)
    return Text(f"{indent}{glyph}{content}", style=style)


def log_line(text: str, style: str | None = None) -> None:
    """Render one operation log record to the console.

    Multi-line records are handled line by line; pure dividers and redundant
    rows are dropped. If ``style`` is given it overrides the inferred styling
    and the text is printed verbatim (paths still shortened).
    """
    if style is not None:
        console.print(Text(_shorten_paths(text), style=style),
                      markup=False, highlight=False)
        return

    pieces = [_format_physical_line(ln) for ln in text.split("\n")]
    pieces = [p for p in pieces if p is not None]
    if not pieces:
        return
    out = pieces[0]
    for p in pieces[1:]:
        out.append("\n")
        out.append(p)
    console.print(out, markup=False, highlight=False)


def rule(title, style: str = "section") -> None:
    """Render a titled horizontal rule (general purpose, used by the CLI)."""
    console.print()
    console.rule(Text(str(title), style=style), style=style, characters="─")


def _pretty_section(name: str) -> str:
    """'[generate_file_list]' -> 'generate file list'."""
    return name.strip().strip("[]").replace("_", " ").strip()


def section_start(name: str) -> None:
    """Render a modern titled rule marking the start of a processing phase."""
    console.print()
    console.rule(Text(_pretty_section(name), style="section"),
                 style="section", characters="─")


def section_end(name: str, elapsed: float) -> None:
    """Render a subtle completion line for a processing phase."""
    console.print(Text(f"  ✓ {_pretty_section(name)} · {elapsed:.2f}s",
                       style="muted"), markup=False, highlight=False)


def startup_panel(title: str, facts: dict) -> None:
    """Render the run header: a panel with the key facts about this run."""
    grid = Table.grid(padding=(0, 2))
    grid.add_column(style="muted", justify="right")
    grid.add_column(style="info")
    for label, value in facts.items():
        grid.add_row(label, _shorten_paths(str(value)))
    console.print()
    console.print(Panel(grid, title=Text(title, style="heading"),
                        border_style="section", box=box.ROUNDED,
                        title_align="left", padding=(1, 2)))


def settings_table(settings: dict) -> None:
    """Render all run settings as a clean key/value card.

    Every setting is shown (no information dropped); values fold rather than
    truncate, and absolute paths are shortened.
    """
    grid = Table.grid(padding=(0, 3))
    grid.add_column(style="muted", justify="right", no_wrap=True)
    grid.add_column(style="info", overflow="fold")
    for key, value in settings.items():
        grid.add_row(key, _shorten_paths(str(value)))
    console.print(Panel(grid, title=Text("settings", style="muted"),
                        border_style="muted", box=box.ROUNDED,
                        title_align="left", padding=(1, 2)))
    console.print()
