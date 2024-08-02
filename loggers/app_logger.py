import logging


class AppLogger:
    _logger = None

    @staticmethod
    def get_logger():
        if AppLogger._logger is None:
            logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
            AppLogger._logger = logging.getLogger(__name__)
        return AppLogger._logger
