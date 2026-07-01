"""
Command-line interface for ppicos.

This module provides the CLI entry point for ppicos, enabling users to run
the data processing pipeline from the command line with flexible options for
selecting specific file types, instances, and processing parameters.

Usage Examples:
    # List available file types
    ppicos --list

    # Run all file types
    ppicos

    # Run specific file type
    ppicos --type 10_meteo

    # Run forest floor instance 2
    ppicos --type 12_meteo_forest_floor --instance 2

    # Run with custom search window (30 days instead of default 14)
    ppicos --max-age-days 30

    # Show help and all available options
    ppicos --help

Available File Types:
    - 10_meteo: Basic meteorology
    - 10_meteo_press: Pressure sensor
    - 10_meteo_heatflag_sonic: Heat flag & sonic anemometer
    - 11_meteo_hut_prec: Hut precipitation
    - 12_meteo_forest_floor: Forest floor (instances 1-5)
    - 13_meteo_meteoswiss: MeteoSwiss data
    - 13_meteo_backup_eth: ETH backup data
    - 13_meteo_nabel: NABEL data
    - 15_meteo_snowheight: Snow height
    - 17_meteo_profile: Meteorology profile
    - 30_profile_ghg: GHG profile

Exit Codes:
    0: Success (all processors completed without errors)
    1: Failure (at least one processor failed or invalid arguments)
"""

import argparse
import datetime
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from ppicos import __version__, filesettings, richconsole, tools
from ppicos.main import IcosFormat, NoFilesFoundError

# Stderr console for errors/warnings so they stay on the error stream and
# remain clearly visible. Shares the same theme as the main console.
error_console = Console(stderr=True, theme=richconsole.THEME, highlight=False)


AVAILABLE_TYPES = {
    '10_meteo': (filesettings.f_10_meteo, {}),
    'localtest_f_10_meteo': (filesettings.localtest_f_10_meteo, {}),
    '10_meteo_press': (filesettings.f_10_meteo_press, {}),
    '10_meteo_heatflag_sonic': (filesettings.f_10_meteo_heatflag_sonic, {}),
    '11_meteo_hut_prec': (filesettings.f_11_meteo_hut_prec, {}),
    '12_meteo_forest_floor': (filesettings.f_12_meteo_forest_floor, {'forest_floor': None, 'table': 1}),
    '13_meteo_meteoswiss': (filesettings.f_13_meteo_meteoswiss, {}),
    '13_meteo_backup_eth': (filesettings.f_13_meteo_backup_eth, {}),
    '13_meteo_nabel': (filesettings.f_13_meteo_nabel, {}),
    '15_meteo_snowheight': (filesettings.f_15_meteo_snowheight, {}),
    '17_meteo_profile': (filesettings.f_17_meteo_profile, {}),
    '30_profile_ghg': (filesettings.f_30_profile_ghg, {}),
}


def _is_localtest(filetype):
    """Local-testing types point at local example data. They stay runnable via
    --type but are hidden from --list and skipped by the run-all batch."""
    return 'localtest' in filetype


def main():
    """Main CLI entry point for ppicos"""
    parser = argparse.ArgumentParser(
        description='Post-processing for ICOS flux tower data',
        prog='ppicos',
        epilog='Examples:\n'
               '  ppicos                                    # Run all file types (3 workers)\n'
               '  ppicos --workers 5                        # Run all with 5 parallel workers\n'
               '  ppicos --dry-run                          # Preview all steps, create nothing\n'
               '  ppicos --type 10_meteo                    # Run specific processor\n'
               '  ppicos --type 12_meteo_forest_floor --instance 2  # Run forest floor 2\n'
               '  ppicos --list                             # List available types\n',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument(
        '--type', '-t',
        dest='filetype',
        help='Specific file type to process (see --list for options)'
    )
    parser.add_argument(
        '--instance',
        type=int,
        help='For multi-instance types (12_meteo_forest_floor), specify instance (1-5)'
    )
    parser.add_argument(
        '--table',
        type=int,
        default=1,
        help='For multi-table types, specify table number (default: 1)'
    )
    parser.add_argument(
        '--max-age-days',
        type=int,
        default=14,
        help='Maximum age of files to process in days (default: 14)'
    )
    parser.add_argument(
        '--workers', '-w',
        type=int,
        default=3,
        help='Number of file types to process in parallel when running all '
             '(default: 3, one worker per file type)'
    )
    parser.add_argument(
        '--dry-run', '-n',
        action='store_true',
        help='Preview all steps without creating or modifying any files'
    )
    parser.add_argument(
        '--list', '-l',
        action='store_true',
        help='List all available file types and exit'
    )

    args = parser.parse_args()

    # Handle --list
    if args.list:
        print_available_types()
        return 0

    # Run processors
    try:
        if args.filetype:
            return run_specific_processor(args.filetype, args.instance, args.table,
                                          args.max_age_days, dry_run=args.dry_run)
        else:
            return run_all_processors(args.max_age_days, workers=args.workers,
                                      dry_run=args.dry_run)
    except Exception as e:
        error_console.print(f"Error: {e}", style="error")
        return 1


def print_available_types():
    """Print list of available file types"""
    descriptions = {
        '10_meteo': 'Basic meteorology',
        'localtest_f_10_meteo': 'Basic meteorology (LOCAL TESTING ONLY)',
        '10_meteo_press': 'Pressure sensor',
        '10_meteo_heatflag_sonic': 'Heat flag & sonic anemometer',
        '11_meteo_hut_prec': 'Hut precipitation',
        '12_meteo_forest_floor': 'Forest floor (instances 1-5)',
        '13_meteo_meteoswiss': 'MeteoSwiss data',
        '13_meteo_backup_eth': 'ETH backup data',
        '13_meteo_nabel': 'NABEL data',
        '15_meteo_snowheight': 'Snow height',
        '17_meteo_profile': 'Meteorology profile',
        '30_profile_ghg': 'GHG profile',
    }
    table = Table(title="Available file types", title_style="heading",
                  header_style="section", border_style="muted")
    table.add_column("File type", style="accent", no_wrap=True)
    table.add_column("Description", style="info")
    for name in sorted(AVAILABLE_TYPES.keys()):
        if _is_localtest(name):
            continue
        table.add_row(name, descriptions.get(name, ''))
    richconsole.console.print()
    richconsole.console.print(table)
    richconsole.console.print()


def run_specific_processor(filetype, instance, table, max_age_days, dry_run=False):
    """Run a specific processor"""
    if filetype not in AVAILABLE_TYPES:
        error_console.print(f"Error: Unknown file type '{filetype}'", style="error")
        error_console.print("Use 'ppicos --list' to see available types", style="muted")
        return 1

    func, kwargs = AVAILABLE_TYPES[filetype]

    # Handle multi-instance types
    if filetype == '12_meteo_forest_floor':
        if instance is None:
            error_console.print("Error: --instance required for 12_meteo_forest_floor", style="error")
            error_console.print("Valid instances: 1-5", style="muted")
            return 1
        if not (1 <= instance <= 5):
            error_console.print(f"Error: Invalid instance {instance}. Valid range: 1-5", style="error")
            return 1
        kwargs['forest_floor'] = instance
        kwargs['table'] = table
        display_name = f"{filetype}_instance_{instance}"
    else:
        if instance is not None:
            error_console.print(f"Warning: --instance ignored for {filetype}", style="warning")
        display_name = filetype

    banner = f"Running: {display_name}"
    if dry_run:
        banner += "  [DRY RUN — no files will be created or modified]"

    try:
        richconsole.console.print()
        richconsole.console.print(
            Panel(banner, style="heading", border_style="section")
        )
        richconsole.console.print()

        settings = func(**kwargs) if kwargs else func()
        icosformat = IcosFormat(filesettings=settings, max_age_days=max_age_days,
                                dry_run=dry_run)
        icosformat.run()

        richconsole.console.print()
        done = "Dry run complete" if dry_run else "Successfully completed"
        richconsole.console.print(f"✓ {done}: {display_name}", style="success")
        richconsole.console.print()
        return 0
    except NoFilesFoundError as e:
        richconsole.console.print()
        richconsole.console.print(f"· No files to process: {display_name} ({e})", style="muted")
        richconsole.console.print()
        return 0
    except Exception as e:
        error_console.print()
        error_console.print(f"✗ Failed: {display_name}", style="error")
        error_console.print(f"Error: {e}", style="error")
        error_console.print()
        return 1


def _build_processor_list():
    """Expand AVAILABLE_TYPES into (display_name, func, kwargs) work items,
    excluding local-test types and expanding forest floors into instances."""
    processors = []
    for filetype, (func, kwargs) in AVAILABLE_TYPES.items():
        if _is_localtest(filetype):
            continue
        if filetype == '12_meteo_forest_floor':
            for instance in range(1, 6):
                display_name = f"{filetype}_instance_{instance}"
                kwargs_copy = kwargs.copy()
                kwargs_copy['forest_floor'] = instance
                kwargs_copy['table'] = 1
                processors.append((display_name, func, kwargs_copy))
        else:
            processors.append((filetype, func, kwargs))
    return processors


def _run_one(display_name, func, kwargs, max_age_days, dry_run, echo):
    """Run a single file type.

    Returns (display_name, outcome, elapsed_seconds, error) where outcome is
    one of 'success', 'no_files' or 'failed'. Never raises, so it is safe to
    hand to a worker pool.
    """
    start = datetime.datetime.now()
    try:
        settings = func(**kwargs) if kwargs else func()
        IcosFormat(filesettings=settings, max_age_days=max_age_days,
                   dry_run=dry_run, echo_console=echo).run()
        outcome, error = 'success', None
    except NoFilesFoundError as e:
        outcome, error = 'no_files', str(e)
    except Exception as e:
        outcome, error = 'failed', str(e)
    elapsed = (datetime.datetime.now() - start).total_seconds()
    return display_name, outcome, elapsed, error


def _print_status(name, outcome, elapsed, error):
    """Print one compact status line for a completed parallel run."""
    if outcome == 'success':
        richconsole.console.print(f"✓ {name} · {elapsed:.1f}s", style="success")
    elif outcome == 'no_files':
        richconsole.console.print(f"· {name} — no files in window", style="muted")
    else:
        richconsole.log_line(f"✗ {name} — {error}", style="error")


def _print_names_table(title, names, style):
    # Title on its own line so it never wraps to the (narrow) name column width
    richconsole.console.print()
    richconsole.console.print(title, style=style)
    if not names:
        return
    table = Table(show_header=False, border_style="muted", box=box.ROUNDED)
    table.add_column("File type", style=style)
    for n in names:
        table.add_row(n)
    richconsole.console.print(table)


def _print_intro():
    """Print a short header explaining what ppicos is and what it does."""
    intro = (
        "Post-processing for ICOS — converts raw data recorded at the "
        "CH-DAV flux tower (Davos, Switzerland) into ICOS-compliant CSV "
        "files for submission to the ICOS network.\n\n"
        "No raw values are changed; only formatting is applied (column "
        "renaming, timestamp reformatting, suffix removal). For each file "
        "type it searches the source folders, reads the matching raw files, "
        "reformats them, and writes one CSV per day."
    )
    richconsole.console.print()
    richconsole.console.print(
        Panel(intro, title=Text(f"ppicos v{__version__}", style="heading"),
              border_style="section", box=box.ROUNDED,
              title_align="left", padding=(1, 2))
    )


def run_all_processors(max_age_days, workers=3, dry_run=False):
    """Run every file type (except local-test types).

    Normal mode runs file types concurrently, one worker per file type; each
    run's detail goes to its own log file while the console shows a compact
    status line per completion. Dry-run mode runs sequentially and prints a
    full, ordered preview without creating or modifying any files.
    """
    script_start = datetime.datetime.now()
    processors = _build_processor_list()

    _print_intro()

    first_date, last_date = tools.set_search_window(max_age_days=max_age_days)
    facts = {
        "Mode": "DRY RUN — no files created or modified" if dry_run else "Live run",
        "File types": len(processors),
        "Max age": f"{max_age_days} days",
        "Search window": f"{first_date} → {last_date}",
        "Workers": "1 (sequential)" if dry_run else workers,
    }
    title = "Run settings (dry run)" if dry_run else "Run settings"
    richconsole.startup_panel(title, facts)

    results = []
    if dry_run:
        # Sequential, verbose, ordered preview (readability over speed)
        for display_name, func, kwargs in processors:
            richconsole.rule(f"[dry-run] {display_name}")
            results.append(_run_one(display_name, func, kwargs, max_age_days,
                                    dry_run=True, echo=True))
    else:
        # Parallel, quiet workers; the main thread prints status on completion
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [executor.submit(_run_one, display_name, func, kwargs,
                                       max_age_days, False, False)
                       for display_name, func, kwargs in processors]
            for future in as_completed(futures):
                result = future.result()
                _print_status(*result)
                results.append(result)

    # Summary
    successful = [r[0] for r in results if r[1] == 'success']
    no_files = [r[0] for r in results if r[1] == 'no_files']
    failed = [(r[0], r[3]) for r in results if r[1] == 'failed']

    total_seconds = datetime.datetime.now() - script_start
    richconsole.console.print()
    richconsole.console.print(
        Panel(f"Runtime for all file types: {total_seconds}",
              style="heading", border_style="section")
    )

    verb = "Would run" if dry_run else "Completed"
    _print_names_table(f"✓ {verb} ({len(successful)})", successful, "success")

    if no_files:
        _print_names_table(f"· No files in window ({len(no_files)})", no_files, "muted")

    if failed:
        failed_table = Table(title=f"✗ Failed ({len(failed)})",
                             title_style="error", border_style="muted")
        failed_table.add_column("File type", style="error", no_wrap=True)
        failed_table.add_column("Error", style="error", overflow="fold")
        for name, error in failed:
            failed_table.add_row(name, error or "")
        richconsole.console.print()
        richconsole.console.print(failed_table)
    else:
        richconsole.console.print()
        richconsole.console.print("No failures.", style="success")

    # One-line overview at the very end
    action = "previewed" if dry_run else "processed"
    runtime = str(total_seconds).split('.')[0]  # drop microseconds
    overview = (f"Overview: {len(results)} file types {action} · "
                f"{len(successful)} with output · "
                f"{len(no_files)} no files · "
                f"{len(failed)} failed · runtime {runtime}")
    overview_style = "error" if failed else "success"
    richconsole.console.print()
    richconsole.console.print(
        Panel(overview, style=overview_style, border_style="section")
    )

    return 0 if not failed else 1


if __name__ == '__main__':
    sys.exit(main())
