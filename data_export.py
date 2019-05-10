#!/usr/bin/env python
# coding=utf-8

import pandas as pd
from sqlalchemy import create_engine, pool
import pymysql
import argparse
import subprocess
import datetime
import sys
import os
import re
import schema_check
import traceback
from conf import config
from utility import get_engine_str, get_yesterday, is_valid

Dlogger = config.get_logger("DataExport")


def pre_args():
    parse = argparse.ArgumentParser(prog="DataExport", description="I am help message...默认模式是把数据导入到临时表，然后rename为正式表。Example1: python3 data_export.py -w xiaonuan_ddl --db xiaonuan --tb syscategory   Example2: python3 data_export.py -w xiaonuan_ddl --s data_xiaonuan_final.syscategory --t syscategory --mode=overwrite")
    parse.add_argument("-w", "--wizard", required=True, help="wizard,选择已经添加的数据库配置名称. example: -w xiaonuan_ddl")
    parse.add_argument("--db", default="", help="<database> meta_export中的db_name库名,不区分大小写")
    parse.add_argument("--tb", default="", help="<table_name> meta_export中的table_name表名,不区分大小写")
    parse.add_argument("--mode", choices=["rename", "overwrite", "append"], default="rename", help="导入模式")
    parse.add_argument("--exec_engine", choices=["sqoop", "datax"], default="sqoop", help="执行引擎, sqoop或者datax")
    parse.add_argument("-s", "--source_table", default="", help="source table. example: pet_medical.ods_pmsweb_ppets")
    parse.add_argument("-t", "--target_table", default="", help="target table. example: xiaonuan.ppets")
    parse.add_argument("-m", "--num_pappers", default="1", help="map并行数,默认1个")
    parse.add_argument("--use_local_mode", action="store_true", help="本地模式执行. 集群只有一台机器有外网, 如果分布的任务到没有外网的机器上就不能执行, 就需要指定本地模式。外网的任务建议使用datax引擎")

    args = parse.parse_args()
    print(args)

    args_dict = {
        "connection_id": "",
        "connection_name": "",
        "db_type": "",
        "host": "",
        "user": "",
        "password ": "",
        "port": 0,
        "jdbc_extend": "",
        "default_db": "",
        "hive_database": "",
        "hive_table": "",
        "db_name": "",
        "table_name": "",
        "m": 1,
        "is_overwrite": "",
        "is_drop": "0",
        "mode": "rename",
        "exec_engine": "sqoop"
    }

    wizard_name = args.wizard
    if args.source_table and len(args.source_table.split(".")) != 2:
        print("-s的参数必须是库名加表名，例如:pet_medical.ods_pmsweb_ppets")
        sys.exit(1)
    if args.target_table and len(args.target_table.split(".")) != 2:
        print("-t的参数必须是库名加表名，例如:xiaonuan.ppets")
        sys.exit(1)

    db = args.db.lower()
    tb = args.tb.lower()
    args_dict["exec_engine"] = args.exec_engine
    args_dict["hive_database"] = args.source_table.split(".")[0].lower() if args.source_table else ""
    args_dict["hive_table"] = args.source_table.split(".")[1].lower() if args.source_table else ""
    args_dict["db_name"] = args.target_table.split(".")[0].lower() if args.target_table else ""
    args_dict["table_name"] = args.target_table.split(".")[1].lower() if args.target_table else ""
    args_dict["m"] = args.num_pappers
    args_dict["mode"] = args.mode
    args_dict["use_local_mode"] = "1" if args.use_local_mode else "0"
    # args_dict["is_overwrite"] = "1" if args.hive_overwrite else "0"

    sql = """
    SELECT t.connection_id,
           t.connection_name,
           t.db_type,
           t.host,
           t.user,
           t.password ,
           t.port,
           t.jdbc_extend,
           t.default_db,
           s.hive_database,
           s.hive_table,
           s.db_name,
           s.table_name,
           s.exec_engine,
           s.m,
           s.is_overwrite,
           s.is_drop,
           s.mode
      FROM meta_connections t
      LEFT JOIN meta_export s ON t.connection_id = s.connection_id
     ORDER BY t.connection_id
    """
    con = create_engine(get_engine_str("mysql").format(**config.DB_CONF), poolclass=pool.NullPool)
    df = pd.read_sql(sql=sql, con=con)
    df["db_name"] = df["db_name"].map(lambda x: str(x).lower())
    df["table_name"] = df["table_name"].map(lambda x: str(x).lower())
    df["hive_database"] = df["hive_database"].map(lambda x: str(x).lower())
    df["hive_table"] = df["hive_table"].map(lambda x: str(x).lower())

    conn_names = df["connection_name"].drop_duplicates(keep="first").tolist()
    db_names = df[(df["db_name"].notna()) & (df["db_name"] != "")].drop_duplicates(keep="first")["db_name"].tolist()
    table_names = df[(df["table_name"].notna()) & (df["table_name"] != "")].drop_duplicates(keep="first")["table_name"].tolist()
    # print([x.lower() for x in table_names])
    if wizard_name not in conn_names:
        print("Error Message: -w 数据库链接名称不存在")
        sys.exit(1)
    if db != "" and db.lower() not in [x.lower() for x in db_names]:
        print("Error Message: --db 库名不存在")
        sys.exit(1)
    if tb != "" and tb.lower() not in [x.lower() for x in table_names]:
        print("Error Message: --tb 表名不存在")
        sys.exit(1)
    if tb != "" and args_dict["table_name"] != "":
        print("Error Message: --tb -s 不能同时指定")
        sys.exit(1)

    # print(conn_names, table_names)
    # tmp_row = [row for row in rows if wizard_name == row["connection_name"]][0]
    tmp_row = df[df["connection_name"] == wizard_name].to_dict("records")[0]
    args_dict["connection_id"] = tmp_row["connection_id"]
    args_dict["connection_name"] = tmp_row["connection_name"]
    args_dict["db_type"] = tmp_row["db_type"]
    args_dict["host"] = tmp_row["host"]
    args_dict["user"] = tmp_row["user"]
    args_dict["password"] = tmp_row["password"]
    args_dict["port"] = tmp_row["port"]
    args_dict["jdbc_extend"] = tmp_row["jdbc_extend"]
    args_dict["default_db"] = tmp_row["default_db"]
    if db != "" and tb != "":
        # args_dict = df[(df["connection_name"] == wizard_name) & (df["table_name"] == table_name_meta.lower())].head(1).to_dict("records")[0]
        args_dict = df[(df["connection_name"] == wizard_name) & (df["db_name"] == db) & (df["table_name"] == tb)]
        # 将pandas的特有类型nan处理成原生的None
        args_dict = args_dict.where(args_dict.notna(), None)
        # DataFrame转换成dict
        args_dict = args_dict.to_dict("records")[0]
        # print(df.dtypes)
        # print(args_dict)

    if (db != "" and tb != "") or (args_dict["db_name"] != "" and args_dict["table_name"] != "" and args_dict["hive_database"] != "" and args_dict["hive_table"] != ""):
        pass
    else:
        print("Error Message: 必须指定 -w --db --tb 或者 -w -d -s -t 的参数值")
        sys.exit(1)
    # args_dict["hive_database"] = args_dict["hive_database"]
    # args_dict["hive_table"] = args_dict["hive_table"]
    args_dict["hive_full_name"] = args_dict["hive_database"] + "." + args_dict["hive_table"]
    args_dict["db_conf"] = {
        "host": args_dict["host"],
        "port": args_dict["port"],
        "user": args_dict["user"],
        "password": args_dict["password"],
        "database": args_dict["db_name"],
        "charset": "utf8"
    }
    if not is_valid(args_dict["db_name"]):
        args_dict["db_name"] = args_dict["default_db"]
    if not is_valid(args_dict["mode"]):
        args_dict["mode"] = "rename"
    if not is_valid(args_dict["exec_engine"]):
        args_dict["exec_engine"] = "sqoop"
    if not is_valid(args_dict["m"]):
        args_dict["m"] = 1
    if args.num_pappers != '1':
        args_dict["m"] = args.num_pappers
    args_dict["m"] = int(args_dict["m"])
    print(args_dict)
    return args_dict


def sqoop_export(db_conf, hive_database, hive_table, target_tb_name, m):
    sqoop_export_str = """
        sqoop export \\
        --connect 'jdbc:mysql://{host}:{port}/{database}?useUnicode=true&characterEncoding=utf-8' \\
        --username '{username}' \\
        --password '{password}' \\
        --table {target_tb_name} \\
        --hcatalog-database {hive_database} \\
        --hcatalog-table {hive_table} \\
        --input-null-string '\\\\N' \\
        --input-null-non-string '\\\\N' \\
        -m {m}""".format(host=db_conf["host"],
                         port=db_conf["port"],
                         database=db_conf["database"],
                         username=db_conf["user"],
                         password=db_conf["password"],
                         target_tb_name=target_tb_name,
                         hive_database=hive_database,
                         hive_table=hive_table,
                         m=m)

    # 执行sqoop，返回执行结果
    # sh_cmd = '''rows=`hive -e "set hive.mapred.mode=nonstrict; select count(*) from %s;" 2>>%s`; echo $rows;''' % (tb_name, config.LOG_PATH + "/datasize_check.log")
    sh_cmd = sqoop_export_str
    Dlogger.info("shell command = " + sh_cmd)
    subprocess.check_output(sh_cmd, shell=True)


def datax_generate_json(row, target_tb_name):
    # print(str(hive_cols))
    host = row["host"]
    port = row["port"]
    database = row["db_name"]

    # if is_valid(row["columns"]):
    #     columns = ",".join(['"' + x + '"' for x in row["columns"].strip().split(",")])
    #     columns = "[" + columns + "]"
    # else:
    #     columns = '["*"]'
    columns = '["*"]'

    if row["db_type"] == "sqlserver":
        jdbc_driver = "jdbc:sqlserver://{host}:{port};DatabaseName={database}".format(host=host, port=port, database=database)
    elif row["db_type"] == "mysql":
        jdbc_driver = "jdbc:mysql://{host}:{port}/{database}".format(host=host, port=port, database=database)
    elif row["db_type"] == "oracle":
        jdbc_driver = "jdbc:oracle:thin:@{host}:{port}:{database}".format(host=host, port=port, database=database)
    else:
        raise Exception("DATABASE TYPE ERROR !")

    template_json = r"""{
        "setting": {},
        "job": {
            "setting": {
                "speed": {
                     "channel": %s
                }
        },
        "content": [
            {
                "reader": {
                    "name": "hdfsreader",
                    "parameter": {
                      "defaultFS": "%s",
                      "path":"/user/hive/warehouse/%s.db/%s/*",
                      "column": ["*"],
                      "encoding": "UTF-8",
                      "fileType": "orc"
                    }
                },
                "writer": {
                    "name": "%s",
                    "parameter": {
                        "writeMode": "insert",
                        "username": "%s",
                        "password": "%s",
                        "column": %s,
                        "connection": [
                            {
                                "jdbcUrl": "%s",
                                "table": [
                                    "%s"
                                ]
                            }
                        ]
                    }
                }
            }
        ]
    }
}
    """ % (row["m"], config.HDFS, row["hive_database"], row["hive_table"], row["db_type"] + "writer", row["user"], row["password"], columns, jdbc_driver, target_tb_name)
    with open(os.path.join(config.PROJECT_PATH, "conf/datax_json/{}.json".format(row["hive_full_name"])), "w") as f:
        f.write(template_json)
    Dlogger.info("generate datax json succeed")


def datax_export(row):
    datax_cmd = """{python2} {datax} {template_json}""" \
        .format(python2=config.PYTHON2,
                datax=config.DATAX,
                template_json=os.path.join(config.PROJECT_PATH, "conf/datax_json/{}.json".format(row["hive_full_name"]))
                )
    Dlogger.info("Shell Command = " + datax_cmd)
    # subprocess.check_output(datax_cmd, shell=True)
    subprocess.check_call(datax_cmd, shell=True)


def pre_ddl(row, con_t, dt, last_dt):
    mode = row["mode"]
    db_conf = row["db_conf"]
    tb_name = row["table_name"]
    hive_full_name = row["hive_full_name"]

    if mode == "rename":
        target_tb_name = "{}_{}".format(tb_name, dt)
        bak_tb_name = "{}_{}_bak".format(tb_name, last_dt)

        # 可以忽略如下警告 pymysql/cursors.py:170: Warning: (1051, "Unknown table ...")
        import warnings
        warnings.filterwarnings("ignore", category=pymysql.Warning)

        pre_sql = """
        drop table if exists {target_tb_name};
        create table {target_tb_name} like {tb_name};""".format(target_tb_name=target_tb_name, tb_name=tb_name)
        rename_sql = """\n        drop table if exists {bak_tb_name};\n        rename table {tb_name} to {bak_tb_name},{target_tb_name} to {tb_name};""".format(bak_tb_name=bak_tb_name, tb_name=tb_name, target_tb_name=target_tb_name)
    elif mode == "overwrite":
        target_tb_name = tb_name
        pre_sql = """
        truncate table {target_tb_name};""".format(target_tb_name=target_tb_name)
        # pre_sql = """
        #     delete from {target_tb_name}""".format(target_tb_name=target_tb_name)
        rename_sql = ""
    else:  # mode == "append"
        target_tb_name = tb_name
        pre_sql = ""
        rename_sql = ""

    # 1、检测表是否存在，不存在创建。存在继续检测字段顺序，不对应严重警告，但不做任何处理。顺序一样，字段缺失会自动添加执行alter add
    # 2、执行预处理sql语句，pre_sql
    cols_list, mysql_cols, map_column_hive, alter_sql = schema_check.check_hive_to_mysql(hive_full_name, tb_name, db_conf)

    # con_t = pymysql.connect(**db_conf).cursor()
    if len(mysql_cols) == 0:
        Dlogger.info("mysql table {} not exists".format(tb_name))
        schema_sql = "\n        CREATE TABLE {tb_name} (\n{cols}\n        ) ENGINE=MyISAM CHARSET=utf8mb4;".format(tb_name=tb_name, cols=''.join(["            " + x + ",\n" for x in cols_list])[:-2])
        Dlogger.info(schema_sql)
        for tmp_sql in [x for x in schema_sql.split(";") if x]:
            con_t.execute(tmp_sql)
    if len(alter_sql) > 0:
        alter_sql = "".join(["\n        " + x for x in alter_sql])
        Dlogger.info(alter_sql)
        for tmp_sql in [x for x in alter_sql.split(";") if x]:
            con_t.execute(tmp_sql.strip())
    if len(pre_sql) > 0:
        Dlogger.info(pre_sql)
        for tmp_sql in [x for x in pre_sql.split(";") if x]:
            con_t.execute(tmp_sql)
    return rename_sql, target_tb_name


def post_ddl(mode, con_t, rename_sql, tb_name, last_dt):
    # 表重命名和删除备份的表，rename and drop bak table
    if mode == "rename":
        Dlogger.info(rename_sql)
        for tmp_sql in [x for x in rename_sql.split(";") if x]:
            con_t.execute(tmp_sql)
        show_bak_sql = "\n        show tables like '{tb_name}%%';".format(tb_name=tb_name)
        Dlogger.info(show_bak_sql)
        df = pd.read_sql(sql=show_bak_sql.strip(), con=con_t)
        # print(df.dtypes)
        # print(df)
        tb_name_list = df.iloc[:, 0].tolist()
        Dlogger.info(tb_name_list)
        if re.search("_bak", ','.join(tb_name_list)):
            for tmp_tb_name in [x for x in tb_name_list if "_bak" in x]:
                tmp_dt = re.sub("{}_|_bak".format(tb_name), "", tmp_tb_name)
                try:
                    datetime.datetime.strptime(tmp_dt, "%Y%m%d")
                except ValueError:
                    Dlogger.warn(traceback.format_exc())
                    Dlogger.warn("{}的日期格式无效".format(tmp_tb_name))
                else:
                    if tmp_dt < last_dt:
                        drop_bak_sql = "\n        drop table {tmp_tb_name}".format(tmp_tb_name=tmp_tb_name)
                        Dlogger.info(drop_bak_sql)
                        con_t.execute(drop_bak_sql)
                    else:
                        Dlogger.info("{tmp_tb_name} 的日期不小于{last_dt}".format(tmp_tb_name=tmp_tb_name, last_dt=last_dt))
        else:
            Dlogger.warn("{} 没有备份表".format(tb_name))


def main(row):
    con_t = create_engine(get_engine_str("mysql").format(**row["db_conf"]), poolclass=pool.NullPool)
    dt = datetime.datetime.now().strftime("%Y%m%d")
    last_dt = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y%m%d")
    rename_sql, target_tb_name = pre_ddl(row, con_t, dt, last_dt)

    if row["exec_engine"] == "sqoop":
        sqoop_export(row["db_conf"], row["hive_database"], row["hive_table"], target_tb_name, row["m"])
    else:
        datax_generate_json(row, target_tb_name)
        datax_export(row)

    post_ddl(row["mode"], con_t, rename_sql, row["table_name"], last_dt)


if __name__ == "__main__":
    args_dict = pre_args()
    # 开始执行
    start_time = datetime.datetime.now()
    main(args_dict)
    end_time = datetime.datetime.now()
    Dlogger.info("Total Time Taken: {} seconds".format(str((end_time - start_time).seconds)))
