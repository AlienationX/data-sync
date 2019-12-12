#!/usr/bin/env python
# coding=utf-8

import subprocess
import datetime
import os
from conf import config


def is_valid(s):
    if s is not None and str(s).strip() != "":
        return True
    else:
        return False


def get_yesterday():
    return datetime.datetime.strftime(datetime.datetime.now() - datetime.timedelta(days=1), "%Y%m%d")


def get_engine_str(db_type):
    if db_type == "sqlserver":
        engine_str = "mssql+pymssql://{user}:{password}@{host}:{port}/{database}?charset=utf8"
    elif db_type == "mysql":
        engine_str = "mysql+mysqlconnector://{user}:{password}@{host}:{port}/{database}?charset=utf8"
        # engine_str = "mysql+pymysql://{user}:{password}@{host}:{port}/{database}?charset=utf8"
    elif db_type == "oracle":
        engine_str = "oracle+cx_oracle://{user}:{password}@{host}:{port}/{database}"
    elif db_type == "hive":
        engine_str = "hive://{user}@{host}:{port}/default"
    elif db_type == "impala":
        engine_str = "impala://{host}:{port}/default"
    else:
        raise Exception("DATABASE TYPE ERROR !")
    return engine_str


def send_mail(title, msg):
    """ 使用Azkaban调度，权限存在问题，需要切换用户发送 """
    # cmd = """ ssh pet@10.15.1.37 "echo '{msg}' | mutt {email} -s '{title}'" """.format(title=title, msg=msg, email=config.EMAIL)
    # try:
    #     subprocess.check_call(cmd, shell=True)
    #     print("Send Mail Succeed")
    #     # raise Exception("{}和{}字段顺序不一致".format(source_tb_name, target_tb_name))
    # except subprocess.CalledProcessError:
    #     print("Send Mail Failed")

    # ssh切换用户有时候会卡住进程，修改成写文件的形式，第二天再发送日志
    if not os.path.exists(config.LOG_PATH):
        os.makedirs(config.LOG_PATH)
    cmd = """ echo -e '{title} {dt}:\n\n{msg}' >> {log_path}/failed_task.log """.format(title=title, dt=str(datetime.datetime.now()), msg=msg, log_path=config.LOG_PATH)
    try:
        subprocess.check_call(cmd, shell=True)
        print("Write Log Succeed")
        # raise Exception("{}和{}字段顺序不一致".format(source_tb_name, target_tb_name))
    except subprocess.CalledProcessError:
        print("Write Log Failed")

