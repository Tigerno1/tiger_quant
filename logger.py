import os
import logging
import logging.handlers
from concurrent_log_handler import ConcurrentTimedRotatingFileHandler
from typing import TYPE_CHECKING
from colorlog import ColoredFormatter
from conf import LOG_LEVEL, LOG_DIR

LOG_DIR = os.path.expanduser("~/tiger_quant/logs")
LOG_FILENAME = os.path.join(LOG_DIR, "tiger_quant.log")


if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)


class TigerLogger:
    def __init__(self, logger: logging.Logger) -> None:
        self._logger = logger
        self.info = logger.info
        self.debug = logger.debug
        self.warn = logger.warn
        self.error = logger.error
        self.exception = logger.exception

    # def log_orders(self, orders: list["Order"]):
    #     self.info("\n" + "\n".join([str(o) for o in orders]))

    # def log_positions(self, positions: list["Position"]):
    #     self.info("\n" + "\n".join([str(p) for p in positions]))


class Logging:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._logger = None
        return cls._instance

    def get_logger(self) -> TigerLogger:
        if self._logger is not None:
            return self._logger

        # Clean up existing handlers on the root logger.
        # Tushare sneaks in a handler that prints to stdout, which causes duplicate messages
        # get printed to the console...
        root_logger = logging.getLogger()
        for handler in root_logger.handlers:
            root_logger.removeHandler(handler)

        logger = logging.getLogger("tiger_quant")
        logger.setLevel(LOG_LEVEL)

        # Set up file handler
        file_handler = ConcurrentTimedRotatingFileHandler(
            LOG_FILENAME, when="D", interval=1, backupCount=365
        )
        file_handler.setLevel(LOG_LEVEL)
        file_formatter = logging.Formatter(
            "[%(module)s] [%(asctime)s] [%(levelname)s]: %(message)s"
        )

        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)

        # Add colorlog formatter to console handler
        color_formatter = ColoredFormatter(
            fmt="%(log_color)s[%(module)s] [%(asctime)s] [%(levelname)s]: %(message)s%(reset)s"
        )
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(color_formatter)
        console_handler.setLevel(logging.DEBUG)
        logger.addHandler(console_handler)

        self._logger = TigerLogger(logger)
        return self._logger


LOG = Logging().get_logger()