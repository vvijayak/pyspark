#!/usr/bin/python
import logging

class PysparkLogging(object):

    logger = None

    def __init__(self,format = '%(asctime)s [%(levelname)s][%(filename)s] %(message)s',
                 level = 'ERROR'):
        self.logger = logging.getLogger()
        handler = logging.StreamHandler()
        formatter = logging.Formatter(format)
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        if level == 'ERROR':
            self.logger.setLevel(logging.ERROR)
        elif level == 'INFO':
            self.logger.setLevel(logging.INFO)
        else:
            raise ValueError('Unsupported Logging Level. [ERROR, INFO]')

    def get_logger(self):return self.logger