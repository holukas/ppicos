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

from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from ppicos import filesettings, richconsole
from ppicos.main import IcosFormat

# Stderr console for errors/warnings so they stay on the error stream and
# remain clearly visible. Shares the same theme as the main console.
error_console = Console(stderr=True, theme=richconsole.THEME, highlight=False)


AVAILABLE_TYPES = {
    '10_meteo': (filesettings.f_10_meteo, {}),
    '10_meteo_localtest': (filesettings.localtest_f_10_meteo, {}),
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


def main():
    """Main CLI entry point for ppicos"""
    parser = argparse.ArgumentParser(
        description='Post-processing for ICOS flux tower data',
        prog='ppicos',
        epilog='Examples:\n'
               '  ppicos                                    # Run all file types\n'
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
            return run_specific_processor(args.filetype, args.instance, args.table, args.max_age_days)
        else:
            return run_all_processors(args.max_age_days)
    except Exception as e:
        error_console.print(f"Error: {e}", style="error")
        return 1


def print_available_types():
    """Print list of available file types"""
    descriptions = {
        '10_meteo': 'Basic meteorology',
        '10_meteo_localtest': 'Basic meteorology (LOCAL TESTING ONLY)',
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
        table.add_row(name, descriptions.get(name, ''))
    richconsole.console.print()
    richconsole.console.print(table)
    richconsole.console.print()


def run_specific_processor(filetype, instance, table, max_age_days):
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

    try:
        richconsole.console.print()
        richconsole.console.print(
            Panel(f"Running: {display_name}", style="heading", border_style="section")
        )
        richconsole.console.print()

        settings = func(**kwargs) if kwargs else func()
        icosformat = IcosFormat(filesettings=settings, max_age_days=max_age_days)
        icosformat.run()

        richconsole.console.print()
        richconsole.console.print(f"✓ Successfully completed: {display_name}", style="success")
        richconsole.console.print()
        return 0
    except Exception as e:
        error_console.print()
        error_console.print(f"✗ Failed: {display_name}", style="error")
        error_console.print(f"Error: {e}", style="error")
        error_console.print()
        return 1


def run_all_processors(max_age_days):
    """Run all processors"""
    script_start = datetime.datetime.now()

    # Build processor list
    processors = []
    for filetype, (func, kwargs) in AVAILABLE_TYPES.items():
        if filetype == '12_meteo_forest_floor':
            for instance in range(1, 6):
                display_name = f"{filetype}_instance_{instance}"
                kwargs_copy = kwargs.copy()
                kwargs_copy['forest_floor'] = instance
                kwargs_copy['table'] = 1
                processors.append((display_name, func, kwargs_copy))
        else:
            processors.append((filetype, func, kwargs))

    # Run all processors
    run_successful = []
    run_not_successful = []

    for display_name, func, kwargs in processors:
        try:
            richconsole.rule(f"Processing: {display_name}...")
            settings = func(**kwargs) if kwargs else func()
            icosformat = IcosFormat(filesettings=settings, max_age_days=max_age_days)
            icosformat.run()
            run_successful.append(display_name)
        except Exception as e:
            richconsole.log_line(f"Failed: {display_name} - {e}", style="error")
            run_not_successful.append(display_name)

    # Summary
    total_seconds = datetime.datetime.now() - script_start
    richconsole.console.print()
    richconsole.console.print(
        Panel(f"Runtime for all file types: {total_seconds}",
              style="heading", border_style="section")
    )

    success_table = Table(title=f"✓ Successful runs ({len(run_successful)})",
                          title_style="success", border_style="muted",
                          show_header=False)
    success_table.add_column("Run", style="success")
    for r in run_successful:
        success_table.add_row(r)
    richconsole.console.print()
    richconsole.console.print(success_table)

    if run_not_successful:
        failed_table = Table(title=f"✗ Failed runs ({len(run_not_successful)})",
                             title_style="error", border_style="muted",
                             show_header=False)
        failed_table.add_column("Run", style="error")
        for r in run_not_successful:
            failed_table.add_row(r)
        richconsole.console.print()
        richconsole.console.print(failed_table)
    else:
        richconsole.console.print()
        richconsole.console.print("All processors completed successfully!", style="success")

    return 0 if not run_not_successful else 1


if __name__ == '__main__':
    sys.exit(main())
