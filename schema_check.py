#!/usr/bin/env python
# coding=utf-8

from sqlalchemy import create_engine, pool
import pandas as pd
import re
import subprocess
import traceback
from conf import config
from utility import get_engine_str, send_mail

# Dlogger.basicConfig(level=Dlogger.INFO, format='%(asctime)s - %(levelname)s: %(message)s')

Dlogger = config.get_logger("SchemaCheck")


def get_tabs(db_type, db_conf):
    tabs = []
    if db_type == "sqlserver":
        sql = "select name as table_name from sys.tables;"
        Dlogger.info("MSSQL Command = " + sql)
    elif db_type == "mysql":
        sql = "select table_name from information_schema.tables t where t.table_schema='{db_name}'".format(db_name=db_conf["database"])
        Dlogger.info("MySQL Command = " + sql)
    elif db_type == "oracle":
        sql = "select table_name from user_tables"
        Dlogger.info("Oracle command = " + sql)
    else:
        raise Exception("DATABASE TYPE ERROR !")
    engine_str = get_engine_str(db_type).format(**db_conf)
    con = create_engine(engine_str, poolclass=pool.NullPool)
    # df = pd.read_sql(sql=sql, con=con)
    for row in con.execute(sql):
        tabs.append(row["table_name"])
    Dlogger.info(str(tabs))
    return tabs


def get_cols(db_type, tb_name, db_conf):
    cols = []
    engine_str = get_engine_str(db_type).format(**db_conf)
    if db_type == "sqlserver":
        sql = "sp_columns {tb_name}".format(tb_name=tb_name)
        Dlogger.info("MSSQL Command = " + sql)
        # engine_str = "mssql+pymssql://{username}:{password}@{host}:{port}/{database}?charset=utf8".format(**db_conf)
        con = create_engine(engine_str, poolclass=pool.NullPool)
        df = pd.read_sql(sql=sql, con=con)
        for index, row in df.iterrows():
            key = str(row["COLUMN_NAME"])
            value = str(row["TYPE_NAME"].replace(" identity", ""))
            cols.append({"name": key, "type": value})
    elif db_type == "mysql":
        sql = "select t.column_name,data_type,column_type from information_schema.columns t where t.table_schema='{db_name}' and t.table_name='{tb_name}'".format(db_name=db_conf["database"], tb_name=tb_name)
        Dlogger.info("MySQL Command = " + sql)
        # engine_str = "mysql+pymysql://{username}:{password}@{host}:{port}/{database}?charset=utf8".format(**db_conf)
        con = create_engine(engine_str, poolclass=pool.NullPool)
        df = pd.read_sql(sql=sql, con=con)
        for index, row in df.iterrows():
            key = str(row["column_name"])
            value = str(row["column_type"].replace(" unsigned", ""))
            cols.append({"name": key, "type": value})
    elif db_type == "oracle":
        sql = "select column_name,data_type from user_tab_columns t where lower(t.table_name)='{tb_name}'".format(tb_name=tb_name)
        Dlogger.info("Oracle command = " + sql)
        # engine_str = "oracle+cx_oracle://{username}:{password}@{host}:{port}/{database}".format(**db_conf)
        con = create_engine(engine_str, poolclass=pool.NullPool)
        df = pd.read_sql(sql=sql, con=con)
        for index, row in df.iterrows():
            key = str(row["column_name"])
            value = str(row["data_type"])
            cols.append({"name": key, "type": value})
    elif db_type == "hive":
        sql = "desc {tb_name}".format(tb_name=tb_name)
        Dlogger.info("Hive Command = " + sql)
        # engine_str = "hive://{username}@{host}:{port}/default".format(**db_conf)
        con = create_engine(engine_str, poolclass=pool.NullPool)
        try:
            df = pd.read_sql(sql=sql, con=con)
            # columns = df.columns.values.tolist()
            for index, row in df.iterrows():
                if row["col_name"].rstrip().lstrip() == "":
                    break
                key = row["col_name"]
                value = row["data_type"]
                cols.append({"name": key, "type": value})
            if cols[-1]["name"] in ("date", "p_date"):
                cols.pop(-1)
        except Exception as e:
            traceback.format_exc()
            Dlogger.error("{} not exists. ".format(tb_name))
    else:
        raise Exception("DATABASE TYPE ERROR !")
    Dlogger.info(str(cols))
    return cols


def check_db_to_hive(source_tb_name, target_tb_name, db_conf, db_type):
    result = ""
    cols_list_map = []
    alter_sql = []
    map_column_hive = ""
    db_cols = get_cols(db_type, source_tb_name, db_conf)
    # print(db_cols)
    hive_cols = get_cols("hive", target_tb_name, config.HIVE_CONF)
    if len(db_cols) == 0:
        raise Exception("==========================>>> {db_type} {db_name}.{source_tb_name} not exists".format(db_type=db_type, db_name=db_conf["database"], source_tb_name=source_tb_name))
    for i in range(len(db_cols)):
        source_col_name, source_col_type = db_cols[i]["name"], db_cols[i]["type"]
        # if db_type="sqlserver":
        if re.search("int|tinyint|bigint", source_col_type, re.I):
            source_col_cast_type = re.sub("\(.*\)", "", source_col_type)
        elif re.search("char|varchar|text", source_col_type, re.I):
            source_col_cast_type = "string"
        elif re.search("date|time", source_col_type, re.I):
            source_col_cast_type = "string"
        elif re.search("float|double|real|decimal|numeric|money|number", source_col_type, re.I):
            source_col_cast_type = "double"
        elif re.search("bit", source_col_type, re.I):
            source_col_cast_type = "boolean"
        else:
            source_col_cast_type = "string"
        # elif db_type="sqlserver":
        # pass
        # elif db_type="oracle":
        # pass

        cols_list_map.append({"name": source_col_name, "type": source_col_cast_type})
        if re.search("timestamp|image", source_col_type):
            map_column_hive += source_col_name + "=string,"
        if i <= len(hive_cols) - 1:
            target_col_name, target_col_type = hive_cols[i]["name"], hive_cols[i]["type"]
            if source_col_name.lower() == target_col_name.lower():
                if source_col_cast_type == re.sub("\(.*", "", target_col_type):
                    flag = "same"
                else:
                    flag = "diff"
            else:
                result = flag = "diff  <<<==========="
            col_tmp = "{} {}({}), {} {}".format(source_col_name, source_col_type, source_col_cast_type, target_col_name, target_col_type)
            print("        {tb:<80} {col_tmp:<80} {flag}".format(tb=source_tb_name + "." + source_col_name, source_col_name=source_col_name, col_tmp=col_tmp, flag=flag))
        else:
            if len(hive_cols) > 0:
                sql = "alter table {target_tb_name} add columns ({source_col_name} {source_col_cast_type});".format(target_tb_name=target_tb_name, source_col_name=source_col_name, source_col_cast_type=source_col_cast_type)
                print("        {tb:<80} {sql}".format(tb=source_tb_name + "." + source_col_name, sql=sql))
                alter_sql.append(sql)

    if result != "" or len(alter_sql) > 0:
        msg = "\n".join(alter_sql)
        if result != "":
            err_msg = "==========================>>> {}和{}字段顺序不一致,不会执行alter语句.....".format(source_tb_name, target_tb_name)
            Dlogger.error(err_msg)
            msg = msg + "\n" + err_msg
            alter_sql = []

        if db_conf["database"] == "ykchr":  # source_tb_name in ("codeitem", "organization", "reta01", "usra01"):
            # 指定sql或者指定column的情况，则不修改表结构和发邮件通知
            alter_sql = []
        else:
            send_mail("Schema Check Warning", msg)

    return cols_list_map, hive_cols, map_column_hive[:-1], alter_sql


def check_hive_to_mysql(source_tb_name, target_tb_name, db_conf):
    result = ""
    cols_list = []
    alter_sql = []
    map_column_hive = ""
    hive_cols = get_cols("hive", source_tb_name, config.HIVE_CONF)
    mysql_cols = get_cols("mysql", target_tb_name, db_conf)
    if len(hive_cols) == 0:
        raise Exception("==========================>>> hive {} not exists".format(source_tb_name))
    for i in range(len(hive_cols)):
        source_col_name, source_col_type = hive_cols[i]["name"], hive_cols[i]["type"]
        if re.search("int|float|double", source_col_type):
            source_col_cast_type = source_col_type
        elif re.search("string", source_col_type):
            source_col_cast_type = "varchar"
        else:
            source_col_cast_type = "varchar"
        source_col_cast_type_length = source_col_cast_type + "(255)" if source_col_cast_type == "varchar" else source_col_cast_type
        cols_list.append("{} {}".format(source_col_name, source_col_cast_type_length))
        if i <= len(mysql_cols) - 1:
            target_col_name, target_col_type = mysql_cols[i]["name"], mysql_cols[i]["type"]
            if source_col_name == target_col_name:
                if source_col_cast_type == re.sub("\(.*", "", target_col_type):
                    flag = "same"
                elif source_col_cast_type == "varchar" and re.search("char|text", target_col_type, re.I):
                    flag = "same"
                else:
                    flag = "diff"
            else:
                result = flag = "diff  <<<==========="
            col_tmp = "{} {}({}), {} {}".format(source_col_name, source_col_type, source_col_cast_type, target_col_name, target_col_type)
            print("        {tb:<80} {col_tmp:<80} {flag}".format(tb=source_tb_name + "." + source_col_name, source_col_name=source_col_name, col_tmp=col_tmp, flag=flag))
        else:
            if len(mysql_cols) > 0:
                sql = "alter table {target_tb_name} add {source_col_name} {source_col_cast_type};".format(target_tb_name=target_tb_name, source_col_name=source_col_name, source_col_cast_type=source_col_cast_type_length).lower()
                print("        {tb:<80} {sql}".format(tb=source_tb_name + "." + source_col_name, sql=sql))
                alter_sql.append(sql)
    if result != "":
        alter_sql = []
        Dlogger.error("==========================>>> {}和{}字段顺序不一致,不会执行alter语句.....".format(source_tb_name, target_tb_name))
        # raise Exception("{}和{}字段顺序不一致".format(source_tb_name, target_tb_name))
    return cols_list, mysql_cols, map_column_hive, alter_sql


if __name__ == "__main__":
    sql = """
    SELECT t.connection_id,
           t.connection_name,
           t.db_type,
           t.host,
           t.user,
           t.password ,
           t.port,
           t.default_db,
           s.db_name,
           s.table_name,
           s.hive_database,
           s.hive_table
      FROM meta_connections t
      JOIN meta_import s ON t.connection_id = s.connection_id
     WHERE t.status = '1' and s.status = '1'
     ORDER BY t.connection_id;
    """
    engine_str = "mysql+pymysql://{user}:{password}@{host}:{port}/{database}?charset=utf8".format(**config.DB_CONF)
    engine = create_engine(engine_str, poolclass=pool.NullPool)
    df = pd.read_sql(sql=sql, con=engine)
    for index, row in df.iterrows():
        source_tb_name = row["table_name"]
        target_tb_name = row["hive_database"] + "." + row["hive_table"]
        db_conf = {
            "host": row["host"],
            "port": row["port"],
            "user": row["user"],
            "password": row["password"],
            "database": row["db_name"],
            "charset": "utf8"
        }
        db_type = row["db_type"]
        check_db_to_hive(source_tb_name, target_tb_name, db_conf, db_type)
