#!/usr/bin/env python
# coding=utf-8

import datetime
import time
import sys
import traceback
import os
import signal
import subprocess
import pandas as pd
from multiprocessing import Pool
from utility import get_engine_str
from sqlalchemy import create_engine, pool
from conf import config

# 并行数
_process_nums = 6

# 单任务的执行文件
_data_export_py = os.path.join(sys.path[0], "data_export.py")

# 日志路径
_log_path = os.path.join(config.LOG_PATH, "data_export")

if not os.path.exists(_log_path):
    os.makedirs(_log_path)

# mysql数据库链接
con = create_engine(get_engine_str("mysql").format(**config.DB_CONF), poolclass=pool.NullPool)

Dlogger = config.get_logger("DataExportAuto")


def parallel_write_log(no, id, connection_name, db_name, table_name, last_exec_date, retry_count):
    try:
        current_date = str(datetime.date.today())
        if last_exec_date == current_date:
            print("{no:<3} {table_name:<40}      今日已执行成功 skip.".format(no=no, table_name=db_name + "." + table_name))
            return ""

        full_log_path = "{log_path}/sqoop_export_{db_name}_{table_name}.log".format(log_path=_log_path, db_name=db_name, table_name=table_name)
        sh_cmd = "{python} -u {date_export_py} -w {connection_name} --db {db_name} --tb {table_name} &>> {full_log_path}\n". \
            format(python=config.PYTHON3,
                   date_export_py=_data_export_py,
                   connection_name=connection_name,
                   db_name=db_name,
                   table_name=table_name,
                   full_log_path=full_log_path)
        flag = ""

        for i in range(retry_count + 1):
            start_time = datetime.datetime.now()
            if i > 0:
                times = "失败任务{}重新执行 第{}次".format(table_name, str(i))
            else:
                times = ""
            print("{no:<3} {table_name:<40}      StartTime: {start_time}      {times}".format(no=no, table_name=db_name + "." + table_name, start_time=str(start_time), times=times))
            try:
                subprocess.check_output("""echo -e "{sh_cmd}\n" >> {full_log_path}""".format(sh_cmd=sh_cmd, full_log_path=full_log_path), shell=True)
                subprocess.check_output(sh_cmd, shell=True)
                flag = "SUCCEEDED"
            except subprocess.CalledProcessError:
                flag = "FAILED"
            end_time = datetime.datetime.now()
            exec_time = (end_time - start_time).seconds
            print("{no:<3} {table_name:<40}      StartTime: {start_time}      EndTime: {end_time}      ExecTime: {exec_time:<5}      {flag}".format(no=no, table_name=db_name + "." + table_name, start_time=str(start_time), end_time=str(end_time), exec_time=str(exec_time) + "s", flag=flag))
            if flag == "SUCCEEDED":
                break
            else:
                time.sleep(10)

        if flag == "SUCCEEDED":
            update_date(id)
            return ""
        else:
            return sh_cmd
    except Exception as e:
        print(traceback.format_exc())


def update_date(id):
    sql = "update meta_export set last_exec_date='{}' where id={};".format(str(datetime.date.today()), id)
    con.execute(sql)


def get_all_tasks():
    sql = """
    SELECT t.connection_name,
           s.id,
           s.db_name,
           s.table_name,
           s.last_exec_date,
           s.retry_count,
           s.task_order
      FROM meta_connections t
      JOIN meta_export s ON t.connection_id = s.connection_id
     WHERE t.status = '1' AND s.status = '1'
     ORDER BY -s.task_order desc,t.connection_id
    """
    Dlogger.info(sql)
    df = pd.read_sql(sql, con)
    tmp_all = df.to_dict("record")
    # 增加序号
    all_tasks = []
    for i in range(len(tmp_all)):
        tmp_row = tmp_all[i]
        tmp_row["no"] = i + 1
        all_tasks.append(tmp_row)
    return all_tasks


def ctrlC():
    # 如果用sys.exit()在上层有try的情况下达不到直接结束程序的效果
    # os._exit(0)
    sys.exit(1)


if __name__ == "__main__":
    signal.signal(signal.SIGINT, ctrlC)
    signal.signal(signal.SIGTERM, ctrlC)
    s_times = datetime.datetime.now()
    rows = get_all_tasks()
    Dlogger.info("详细日志路径: {}\n".format(_log_path))
    failed_sh_cmd = []
    p = Pool(_process_nums)
    for row in rows:
        err_table = p.apply_async(func=parallel_write_log, args=(row["no"], row["id"], row["connection_name"], row["db_name"], row["table_name"], row["last_exec_date"], row["retry_count"]))
        failed_sh_cmd.append(err_table)
    p.close()
    p.join()
    failed_sh_cmd = [x.get() for x in failed_sh_cmd if x.get() != ""]
    if len(failed_sh_cmd) > 0:
        Dlogger.info("--- Failed Task ---")
        for res in failed_sh_cmd:
            Dlogger.info(res.strip())
        msg = "\n".join(failed_sh_cmd)
        send_mail = """  ssh pet@10.15.1.37 "echo '{msg}' | mutt {email} -c {email_c} -s 'Data Export Failed'" """.format(msg=msg, email=config.EMAIL, email_c=config.EMAIL_C)
        subprocess.check_output(send_mail, shell=True)
    e_times = datetime.datetime.now()
    print("\nTotal Time Taken: {} seconds".format(str((e_times - s_times).seconds)))
