#!/usr/bin/env python
# coding=utf-8

import re
import os
import signal
import time
import sys
import subprocess
import datetime
import traceback
import pandas as pd
from multiprocessing import Pool
from sqlalchemy import create_engine, pool
from conf import config
from utility import get_engine_str, send_mail

# 并行数
_process_nums = 10

# 任务失败重试次数
# _retry_time = 3

# 单任务的执行文件
_data_import_py = os.path.join(sys.path[0], "data_import.py")

# 日志路径
_log_path = os.path.join(config.LOG_PATH, "data_import")

if not os.path.exists(_log_path):
    os.makedirs(_log_path)

# mysql数据库链接
con = create_engine(get_engine_str("mysql").format(**config.DB_CONF), poolclass=pool.NullPool)

Dlogger = config.get_logger("DataImportAuto")


def parallel_write_log(no, id, connection_name, db_name, table_name, exec_engine, warehouse_dir, last_exec_date, retry_count):
    try:
        current_date = str(datetime.date.today())
        db_name = db_name.lower()
        table_name = table_name.lower()
        if last_exec_date == current_date:
            print("{no:<3} {table_name:<40}      今日已执行成功 skip.".format(no=no, table_name=db_name + "." + table_name))
            return ""

        full_log_path = "{log_path}/{exec_engine}_import_{db_name}_{table_name}.log".format(log_path=_log_path, exec_engine=exec_engine, db_name=db_name, table_name=table_name)
        sh_cmd = "{python} -u {data_import_py} -w {connection_name} --db {db_name} --tb {table_name} &>> {full_log_path}\n". \
            format(python=config.PYTHON3,
                   data_import_py=_data_import_py,
                   connection_name=connection_name,
                   db_name=db_name,
                   table_name=table_name,
                   full_log_path=full_log_path)
        flag = ""
        total_count = retry_count + 1
        for i in range(total_count):
            start_time = datetime.datetime.now()
            if i > 0:
                times = "失败任务{}重新执行 第{}次".format(db_name + "." + table_name, str(i))
            else:
                times = ""
            print("{no:<3} {table_name:<40}      StartTime: {start_time}      {times}".format(no=no, table_name=db_name + "." + table_name, start_time=str(start_time), times=times))
            try:
                subprocess.check_output("""echo -e "{sh_cmd}\n" >> {full_log_path}""".format(sh_cmd=sh_cmd, full_log_path=full_log_path), shell=True)
                subprocess.check_output(sh_cmd, shell=True)
                flag = "SUCCEEDED"
                # 删除成功任务
                # delete_success_task(connection_name, db_name, table_name)
            except subprocess.CalledProcessError:
                flag = "FAILED"
            end_time = datetime.datetime.now()
            exec_time = (end_time - start_time).seconds
            print("{no:<3} {table_name:<40}      StartTime: {start_time}      EndTime: {end_time}      ExecTime: {exec_time:<5}      {flag}".format(no=no, table_name=db_name + "." + table_name, start_time=str(start_time), end_time=str(end_time), exec_time=str(exec_time) + "s", flag=flag))
            if flag == "SUCCEEDED":
                break
            else:
                time.sleep(10)
                # 倒数2次开始删除hdfs上的临时文件
                if len(table_name) > 0 and exec_engine == "sqoop" and i >= total_count - 2:
                    if warehouse_dir is not None and warehouse_dir.strip() != "":
                        hadoop_cmd = "hadoop dfs -rm -r /tmp/`whoami`/{warehouse_dir}/{table_name} 2>/dev/null".format(warehouse_dir=warehouse_dir, table_name=table_name)
                    else:
                        hadoop_cmd = "hadoop dfs -rm -r {table_name} 2>/dev/null".format(table_name=table_name)
                    Dlogger.info(hadoop_cmd)
                    os.system(hadoop_cmd)

        if flag == "SUCCEEDED":
            update_date(id)
            return ""
        else:
            return sh_cmd
    except Exception as e:
        print(traceback.format_exc())


def update_date(id):
    sql = "update meta_import set last_exec_date='{}' where id={};".format(str(datetime.date.today()), id)
    # Dlogger.info(sql)
    con.execute(sql)


def get_all_tasks():
    sql = """
    SELECT t.connection_name,
           s.id,
           s.db_name,
           s.table_name,
           s.exec_engine,
           s.warehouse_dir,
           s.last_exec_date,
           s.retry_count,
           s.task_order,
           s.status
      FROM meta_connections t
      JOIN meta_import s ON t.connection_id = s.connection_id
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
        if tmp_row["table_name"].lower() == "warm_deleted":
            tmp_row["no"] = 0
        else:
            tmp_row["no"] = i + 1
        all_tasks.append(tmp_row)
    return all_tasks


def ctrlC():
    # 如果用sys.exit()在上层有try的情况下达不到直接结束程序的效果
    # os._exit(0)
    sys.exit(1)


def import_del_table(row1):
    """ 串行导入warm_deleted数据 """
    for row in row1:
        cmd_result = parallel_write_log(row["no"], row["id"], row["connection_name"], row["db_name"], row["table_name"], row["exec_engine"], row["warehouse_dir"], row["last_exec_date"], row["retry_count"])
        if cmd_result != "":
            print("{} 执行失败. {}".format(row["db_name"] + "." + row["table_name"], cmd_result))
            sys.exit(1)


if __name__ == "__main__":
    signal.signal(signal.SIGINT, ctrlC)
    signal.signal(signal.SIGTERM, ctrlC)
    s_times = datetime.datetime.now()
    rows = get_all_tasks()
    Dlogger.info("详细日志路径: {}\n".format(_log_path))
    row1 = [x for x in rows if x["no"] == 0]
    row2 = [x for x in rows if x["no"] != 0]

    # 1、先导入warm_deleted表的数据
    import_del_table(row1)

    # 2、根据warm_deleted过滤删除的数据，data_unique中会用到
    failed_sh_cmd = []
    p = Pool(_process_nums)
    for row in row2:
        err_table = p.apply_async(func=parallel_write_log, args=(row["no"], row["id"], row["connection_name"], row["db_name"], row["table_name"], row["exec_engine"], row["warehouse_dir"], row["last_exec_date"], row["retry_count"]))
        failed_sh_cmd.append(err_table)
    p.close()
    p.join()
    failed_sh_cmd = [x.get() for x in failed_sh_cmd if x.get() != ""]
    if len(failed_sh_cmd) > 0:
        Dlogger.info("--- Failed Task ---")
        for res in failed_sh_cmd:
            Dlogger.info(res.strip())
        # print("".join(failed_sh_cmd))
        send_mail("Data Import Failed", "\n".join(failed_sh_cmd))
    e_times = datetime.datetime.now()
    Dlogger.info("Total Time Taken: {} seconds".format(str((e_times - s_times).seconds)))
