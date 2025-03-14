import logging


def setup_logger(log_file="app.log"):
    """
    Set up and return a logger that writes logs to both a file and the console.
    """
    logger = logging.getLogger("custom_logger")
    logger.setLevel(logging.DEBUG)

    # Prevent adding multiple handlers if logger is already set up
    if not logger.hasHandlers():
        # File Handler
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))

        # Console Handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter("%(levelname)s - %(message)s"))

        # Add handlers
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    return logger