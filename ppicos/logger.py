import contextlib
import logging
import os
import time


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

    def log_info(self, record):
        """Output record to console and log file"""
        self.logger.info(record)
        print(record)

    @contextlib.contextmanager
    def section(self, name):
        """Context manager for section logging with automatic timing and cleanup"""
        tic = time.time()
        previous_section = self.current_section
        self.current_section = name

        self.log_info(f"\n\n\n{'-' * 80}\n{name}\n{name} SECTION START")
        try:
            yield
        finally:
            elapsed = time.time() - tic
            self.log_info(f'{name} SECTION END. Runtime: {elapsed:.4f}s')
            self.current_section = previous_section
