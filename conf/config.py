#!/usr/bin/env python
# coding=utf-8

from datetime import datetime
import os
import logging
import sys

DB_CONF = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "root",
    "password": "root123",
    "database": "pms",
    "charset": "utf8"
}

HIVE_CONF = {
    "host": "10.15.1.1",
    "port": 10000,
    "user": "lshu",
    # "password": "",
    "database": "default",
    "charset": "utf8"
}

# hadoop集群地址
HDFS = "hdfs://hi-prod-09.ego.com:8020"

# python路径
PYTHON2 = "/home/work/thirdparty/anaconda2/bin/python2"
PYTHON3 = "/home/pet/app/python3.6/bin/python3"

# datax路径
# 官方下载版本，未经改动
# DATAX = "/home/pet/datax/bin/datax.py"
# 重新编译的版本，增加\n,\r,\r\n和\t替换成空格的代码，解决hive由于换行符导致数据错列的问题
DATAX = "/home/pet/app/datax/bin/datax.py"

# email地址
EMAIL = "a@163.com"
EMAIL_C = "b@163.com,c@163.com,d@163.com"

# 日志级别
LOG_LEVEL = logging.DEBUG

# 项目路径  返回 data_sync
PROJECT_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# 日志路径
# LOG_PATH = os.path.join("/home/pet/log/pet_medical_plus", datetime.now().strftime("%Y%m%d"))
LOG_PATH = os.path.join(PROJECT_PATH, "log", datetime.now().strftime("%Y%m%d"))


def get_logger(name):
    # set default logging configuration
    logger = logging.getLogger(name)  # initialize logging class
    logger.setLevel(LOG_LEVEL)  # default log level
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s - %(name)s: %(message)s"))
    logger.addHandler(console_handler)
    #  output to standard output
    # logger.info("I am testing...")
    return logger
