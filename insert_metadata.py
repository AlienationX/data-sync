#!/usr/bin/env python
# coding=utf-8

import sys
from sqlalchemy import create_engine, pool
import pandas as pd
import argparse
from conf import config
from utility import get_engine_str
from schema_check import get_tabs

Dlogger = config.get_logger("InsertMetaData")


def pre_args():
    parse = argparse.ArgumentParser(prog="InsertMetaData", description="I am help message...")
    parse.add_argument("-w", "--wizard", required=True, help="wizard,选择已经添加的数据库配置名称. example: -w warmsoft")
    parse.add_argument("--db", required=True, help="database,指定需要同步的数据库名称")
    parse.add_argument("--target_db", help="指定同步到hive中的库名，不填默认和db相同")
    args = parse.parse_args()
    print(args)

    args_dict = {
        "connection_id": None,
        "connection_name": args.wizard,
        "db_type": "",
        "host": "",
        "user": "",
        "password ": "",
        "port": 0,
        "jdbc_extend": "",
        "default_db": "",
        "db_name": args.db,
        "target_db_name": args.target_db if args.target_db else args.db
    }
    sql = """
    SELECT t.connection_id,
           t.connection_name,
           t.db_type,
           t.host,
           t.user,
           t.password ,
           t.port,
           t.jdbc_extend,
           t.default_db
      FROM meta_connections t
     WHERE t.connection_name = '{}'""".format(args.wizard)
    Dlogger.info(sql)
    con = create_engine(get_engine_str("mysql").format(**config.DB_CONF), poolclass=pool.NullPool)
    df = pd.read_sql(sql=sql, con=con)

    if len(df) > 0:
        tmp_row = df.to_dict("records")[0]
    else:
        print("Error Message: -w 数据库链接名称不存在")
        sys.exit(1)
    args_dict["connection_id"] = tmp_row["connection_id"]
    args_dict["connection_name"] = tmp_row["connection_name"]
    args_dict["db_type"] = tmp_row["db_type"]
    args_dict["host"] = tmp_row["host"]
    args_dict["user"] = tmp_row["user"]
    args_dict["password"] = tmp_row["password"]
    args_dict["port"] = tmp_row["port"]
    args_dict["jdbc_extend"] = tmp_row["jdbc_extend"]
    args_dict["default_db"] = tmp_row["default_db"]
    args_dict["db_conf"] = {
        "host": args_dict["host"],
        "port": args_dict["port"],
        "user": args_dict["user"],
        "password": args_dict["password"],
        "database": args_dict["db_name"],
        "charset": "utf8"
    }
    Dlogger.info(args_dict)
    return args_dict


def main(row):
    table_names = get_tabs(row["db_type"], row["db_conf"])
    Dlogger.info("{}库共有{}个表".format(row["db_name"], len(table_names)))
    if len(table_names) == 0:
        sys.exit(1)
    insert_sql = """
        INSERT INTO meta_import(connection_id, db_name, table_name, hive_database, hive_table, exec_engine, is_overwrite, status) VALUE"""
    for table_name in table_names:
        tmp_value = "('{connection_id}','{db_name}','{table_name}','{hive_database}','{hive_table}','{exec_engine}',{is_overwrite},{status})," \
            .format(connection_id=row["connection_id"],
                    db_name=row["db_name"],
                    table_name=table_name,
                    hive_database="pet_medical",
                    hive_table="src_" + row["target_db_name"] + "_" + table_name,
                    exec_engine="sqoop",
                    is_overwrite=1,
                    status=0)
        insert_sql = insert_sql + "\n        " + tmp_value
    insert_sql = insert_sql[:-1]
    Dlogger.info(insert_sql)
    con = create_engine(get_engine_str("mysql").format(**config.DB_CONF))
    con.execute(insert_sql)
    Dlogger.info("insert success")
    db_map_sql = "select distinct db_name,substr(t.hive_table,5,locate('_', t.hive_table,5)-1-4) as hive_db_name from meta_import t"
    df = pd.read_sql(sql=db_map_sql, con=con)
    print(df)


if __name__ == "__main__":
    args_dict = pre_args()
    main(args_dict)
