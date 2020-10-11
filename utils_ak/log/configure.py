import logging
import logging.handlers

from utils_ak.log.extra_fields_logger import ExtraFieldsLogger

import os
import sys


def get_log_file_name():
    exec_file_path = os.path.abspath(sys.argv[0])

    exec_file_name = os.path.splitext(os.path.basename(exec_file_path))[0]
    exec_file_dir = os.path.dirname(exec_file_path)
    log_dir = os.path.join(exec_file_dir, 'logs')
    try:
        os.stat(log_dir)
    except:
        os.mkdir(log_dir)

    log_file_name = os.path.join(log_dir, f'{exec_file_name}.log')

    return log_file_name


def reset_logging():
    root_logger = logging.getLogger()
    root_logger.handlers = []


def configure_stream_logging(fmt='%(asctime)s %(name)s: %(message)s',
                             stream=True,
                             stream_level=logging.DEBUG,
                             file_level=None,
                             formatter=None,
                             file_stream=False,
                             logs_path=None,
                             stdout=False):

    file_level = file_level or stream_level

    logging.setLoggerClass(ExtraFieldsLogger)

    formatter = formatter or logging.Formatter(fmt=fmt)
    root_logger = logging.getLogger()
    root_logger.handlers = []
    root_logger.setLevel(0)
    if stream:
        handler = logging.StreamHandler(sys.stdout if stdout else None)
        handler.setLevel(stream_level)
        handler.setFormatter(formatter)
        root_logger.addHandler(handler)

    if file_stream:
        logs_path = logs_path or get_log_file_name()
        fileHandler = logging.handlers.TimedRotatingFileHandler(logs_path, when='midnight', backupCount=21)
        fileHandler.setLevel(file_level)
        fileHandler.setFormatter(formatter)
        root_logger.addHandler(fileHandler)


configure_logging = configure_stream_logging

if __name__ == '__main__':
    configure_logging(file_stream=True, file_level='DEBUG', stream_level='INFO')
    logging.info('foo')
    logger = logging.getLogger('My _logging')
    logger.error('bar')
    # reset_logging()
    logger.info('foo')
    logger.error('bar')
    logger.error('bar', extra={'a': 123})
    logger.debug('debug message')