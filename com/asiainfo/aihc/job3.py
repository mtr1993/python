# coding:utf-8
#########################################
# Function:    main frame
# Usage:       apply/python3 robot/job3.py -i job.cfg
# Author:      AIHC DEV TEAM
# Company:     AsiaInfo Inc.
# Version:     1.0
#########################################

import os
import sys
import time
import base64
import configparser
from sys import path
import logging
import json
from logging.handlers import RotatingFileHandler
from optparse import OptionParser

global logger
global roboter

class Cfg(object):
    def __init__(self, filename=None):
        if filename is None:
            sys.exit(1)
        self._path = filename
        self._cfg = configparser.ConfigParser()
        self._cfg.read(self._path)
        self._dict = {}

    def getValue(self, section, key):
        value = None
        try:
            value = self._cfg.get(section, key)
        except:
            pass
        return value

    def setValue(self, section, key,value):
        try:
            self._cfg.set(section, key,value)
            self._cfg.write(open(self._path, 'wb'))
        except:
            pass

    def getOptions(self):
        op = None
        try:
            op = self._cfg.options("options")
        except:
            pass
        return op

    def getItems(self):
        im = []
        try:
            im = self._cfg.items("options")
        except:
            pass
        return im

class Robot(object):
    def __init__(self, mode, config):
        self._debug = mode
        self._config = config
        self._content = ""
        self._stdout = sys.stdout
        if mode == False:
            sys.stdout = open(os.devnull, 'w')

    def domainParam(self, name):
        return os.getenv(name)

    def decodeParam(self, name):
        if os.getenv(name) is not None:
            return str(base64.b64decode(os.getenv(name)), 'utf-8')
        else:
            return None

    def getNodeId(self):
        return os.getenv('NODE_ID')

    def getNodeIp(self):
        return os.getenv('NODE_IP')

    def getDomainId(self):
        return os.getenv('DOMAIN_ID')

    def getRegionId(self):
        return os.getenv('REGION_ID')

    def getServIp(self):
        return os.getenv('NODE_SERV').split(':')[0]

    def getServPort(self):
        return os.getenv('NODE_SERV').split(':')[1]

    def getConfigVal(self, key):
        return self._config.getValue('options', key)

    def getOptions(self, key):
        return self._config.getOptions(key)

    def getItems(self):
        return self._config.getItems()

    def setContent(self, content):
        self._content = content

    def printResult(self):
        if self._debug == False:
            sys.stdout = self._stdout

        print(self._content)

    def error(self, message):
        if self._debug == False:
            sys.stderr = self._stderr
        print(message, sys.stderr)

        if self._debug == False:
            sys.stderr = open(os.devnull, 'w')


if __name__ == '__main__':
    if sys.getdefaultencoding() != 'utf-8':
        sys.setdefaultencoding('utf-8')

    parser = OptionParser(usage="usage: %prog -i config", version="%prog 1.0")

    parser.add_option("-i", "--config",
                      dest="file",
                      help="input configure from file")

    (options, args) = parser.parse_args()

    if options.file == None:
        parser.print_help()
        exit(-1)

    if len(args) == 1 and args[0] == "debug":
        debug = True
    else:
        debug = False

    cfgname = options.file
    cfgpath = 'conf/job/' + cfgname
    config = Cfg(cfgpath)
    job_name = config.getValue('common', 'job_name')

    if job_name is None:
        print('job name is not config')
        exit()

    LOG_FILE = "logs/job.log"
    LOG_LEVEL = logging.INFO
    LOG_FILE_MAX_BYTES = 1024 * 1024
    LOG_FILE_MAX_COUNT = 3
    logger = logging.getLogger('job')
    logger.setLevel(LOG_LEVEL)
    handler = RotatingFileHandler(filename=LOG_FILE, mode='a', maxBytes=LOG_FILE_MAX_BYTES,
                                  backupCount=LOG_FILE_MAX_COUNT)

    log_fmt = '[' + job_name + ']' + '%(asctime)s [%(levelname)s] -> %(message)s'

    formatter = logging.Formatter(fmt=log_fmt)
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    require = config.getValue('common', 'exec_file')
    if require is not None:
        roboter = Robot(debug, config)
        try:
            module = os.path.expandvars(require)
            file = open(module)
            code = compile(file.read(), module, 'exec')
            exec (code)

            roboter.printResult()
        finally:
            if file is not None:
                file.close()

        exit(0)
    else:
        print('The exec file is not exist')
        exit(-1)