import contextlib
import logging
import os
import time


@contextlib.contextmanager
def section(logger, name):
    """Context manager for section logging with timing"""
    tic = time.time()
    logger.log_info(f"\n\n\n{'-' * 80}\n{name}\n{name} SECTION START")
    try:
        yield name
    finally:
        elapsed = time.time() - tic
        logger.log_info(f'{name} SECTION END. Runtime: {elapsed:.4f}s')


class Logger(object):
    def __init__(self, run_id, logdir, filetype):
        super(Logger, self).__init__()

        # create logger
        outfile = os.path.join(logdir, run_id)
        logfile = '{}_{}.log'.format(outfile, filetype)
        logger = logging.getLogger(logfile)
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter('%(message)s')  # create formatter for handlers
        # formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        # create file handler
        fh = logging.FileHandler(logfile, mode='w')  # create file handler
        fh.setLevel(logging.INFO)  # logs info messages and above
        fh.setFormatter(formatter)  # add formatter to the handler
        logger.addHandler(fh)  # add the handler to logger
        self.logger = logger

    def log_info(self, record):
        # outputs to console and log file
        self.logger.info(record)
        print(record)

        return None
