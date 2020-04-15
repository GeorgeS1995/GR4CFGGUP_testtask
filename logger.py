import logging
import configuration as cfg
import os.path as OP
import os
import datetime
import sys


loglvls = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
work_dir = OP.abspath(os.getcwd())
now = datetime.datetime.now()


def log_init():
    logger = logging.getLogger("log")
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    logProps = cfg.Props()
    logProps.load()
    try:
        os.mkdir(OP.normpath(work_dir + "/logs/"))
    except OSError:
        pass
    finally:
        logspath = OP.normpath(work_dir + "/logs/mini_db_" + now.strftime("%Y%m%d%H%M%S") + ".log")

    fh = logging.FileHandler(logspath)
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    if logProps.get_logconsole():
        ch = logging.StreamHandler(sys.stdout)
        ch.setFormatter(formatter)
        logger.addHandler(ch)

    if logProps.get_loglvl() in loglvls:
        logger.setLevel(logProps.get_loglvl())
    else:
        logger.setLevel(logging.INFO)

    return logger


if __name__ == '__main__':
    logger = log_init()
    logger.debug("debug msg test")
    logger.info("info msg test")
    logger.warning("warning msg test")
    logger.error("error msg test")
    logger.critical("critical msg test")
