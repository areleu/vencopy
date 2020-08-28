#-*- coding:utf-8 -*-

__version__ = '0.1.0'
__maintainer__ = 'Benjamin Fuchs 12.11.2019'
__email__ = 'Benjamin.Fuchs@dlr.de'
__birthdate__ = '26.07.2019'
__status__ = 'dev'  # options are: dev, test, prod


import functools
import time
import inspect
import logging.handlers

logger = logging.getLogger('debugger')
logger.setLevel(logging.INFO)
h = logging.handlers.RotatingFileHandler('./debug.log')
logger.addHandler(h)
h = logging.StreamHandler()
logger.addHandler(h)



def logit(f):

    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        logger.debug(f'entering function "{f.__name__}"')
        src = inspect.getsource(f)
        logger.debug(f'source:\n{src}')
        start = time.time()
        try:
            ret = f(*args, **kwargs)
        except Exception as E:
            logger.exception(f'Error during call of function "{f.__name__}"')
            raise E

        end = time.time()
        logger.debug(f'function call took {end-start} ms')
        logger.debug(f'exiting function "{f.__name__}"')
        return ret  # added this line myself since variable ret wasn't used before

    return wrapper

