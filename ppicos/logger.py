import contextlib
import logging
import os
import time

from ppicos import richconsole


class Logger(object):
    """Logger that outputs to both console and file with automatic section tracking"""

    def __init__(self, run_id, logdir, filetype):
        super(Logger, self).__init__()

        # Set up file logging
        outfile = os.path.join(logdir, run_id)
        logfile = '{}_{}.log'.format(outfile, filetype)
        logger = logging.getLogger(logfile)
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter('%(message)s')
        fh = logging.FileHandler(logfile, mode='w')
        fh.setLevel(logging.INFO)
        fh.setFormatter(formatter)
        logger.addHandler(fh)
        self.logger = logger

        # Track current section for context-aware logging
        self.current_section = ''

    def _log_file(self, record):
        """Write a record to the plain-text log file only (no console output)"""
        self.logger.info(record)

    def log_info(self, record):
        """Output record to console (Rich) and log file"""
        self.logger.info(record)
        richconsole.log_line(record)

    @contextlib.contextmanager
    def section(self, name):
        """Context manager for section logging with automatic timing and cleanup"""
        tic = time.time()
        previous_section = self.current_section
        self.current_section = name

        # File keeps the verbose banner; console gets a modern section rule.
        self._log_file(f"\n\n\n{'-' * 80}\n{name}\n{name} SECTION START")
        richconsole.section_start(name)
        try:
            yield
        finally:
            elapsed = time.time() - tic
            # File keeps the original wording; console gets a subtle summary.
            self._log_file(f'{name} SECTION END. Runtime: {elapsed:.4f}s')
            richconsole.section_end(name, elapsed)
            self.current_section = previous_section

    def startup(self, title, facts: dict, settings: dict, source_dir):
        """Log the run header.

        File: the original verbose banner + key/value header + full settings
        dump (unchanged). Console: a modern header panel and a compact settings
        card (all settings shown, paths shortened).
        """
        # --- log file: identical content to the original _setup_logger ---
        self._log_file('\n\n\n\n\n{s}\n\n     {f}\n\n{s}'.format(s='=' * 120, f=title))
        self._log_file('FILETYPE:      {}'.format(title))
        self._log_file('FILETYPE ID:   {}'.format(facts.get('ID')))
        self._log_file('START TIME:    {}'.format(facts.get('Start')))
        self._log_file('RUN ID:        {}'.format(facts.get('Run ID')))
        self._log_file('LOG FILE PATH: {}'.format(facts.get('Log file')))
        self._log_file("\n" + "-" * 60)
        self._log_file("FOUND SETTINGS FOR THIS RUN")
        for key, value in settings.items():
            self._log_file("{}: {}".format(key, value))
        self._log_file("-" * 60)
        self._log_file('\nsource dir:  {}'.format(source_dir))

        # --- console: compact, modern rendering ---
        richconsole.startup_panel(title, facts)
        richconsole.settings_table(settings)
