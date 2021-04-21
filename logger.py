import logging


class Logger:
    def __init__(self, log_path, file_type):
        self.log_path = log_path
        self.logger = logging.getLogger('debug_log')
        self.logger.setLevel(logging.DEBUG)

        self.fh = logging.FileHandler(self.log_path)
        self.fh.setLevel(logging.DEBUG)

        self.ch = logging.StreamHandler()
        self.ch.setLevel(logging.ERROR)

        self.formatter = logging.Formatter(
            f'{file_type}: %(levelname)s: %(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S')

        self.ch.setFormatter(self.formatter)
        self.fh.setFormatter(self.formatter)

        self.logger.addHandler(self.ch)
        self.logger.addHandler(self.fh)

    def log_debug(self, msg):
        self.logger.debug(msg)

    def log_info(self, msg):
        self.logger.info(msg)

    def log_warning(self, msg):
        self.logger.warning(msg)

    def log_error(self, msg):
        self.logger.error(msg)

    def log_critical(self, msg):
        self.logger.critical(msg)
