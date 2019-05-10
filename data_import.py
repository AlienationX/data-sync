#!/usr/bin/env python
# coding=utf-8

import pandas as pd
from sqlalchemy import create_engine, pool
import argparse
import subprocess
import datetime
import sys
import os
import re
import schema_check
from conf import config
from utility import get_engine_str, get_yesterday, is_valid
from data_unique import drop_duplicates

Dlogger = config.get_logger("DataImport")


def pre_args():
    parse = argparse.ArgumentParser(prog="DataImport", description="I am help message...")
    parse.add_argument("-w", "--wizard", required=True, help="wizard,选择已经添加的数据库配置名称. example: -w warmsoft")
    parse.add_argument("--db", default="", help="<database> meta_import中的db_name库名,不区分大小写")
    parse.add_argument("--tb", default="", help="<table_name> meta_import中的table_name表名,不区分大小写")
    parse.add_argument("--exec_engine", choices=["sqoop", "datax"], default="sqoop", help="执行引擎, sqoop或者datax")
    parse.add_argument("-s", "--source_table", default="", help="source table. example: pms.ppets")
    parse.add_argument("-t", "--target_table", default="", help="target table. example: pet_medical.src_pms_ppets")
    parse.add_argument("-m", "--num_pappers", default="1", help="map并行数,默认1个")
    parse.add_argument("--hive_overwrite", action="store_true", help="sqoop覆盖")
    parse.add_argument("--hive_partition_key", help="分区键")
    parse.add_argument("--hive_partition_value", help="分区值")
    parse.add_argument("--use_local_mode", action="store_true", help="本地模式执行. 集群只有一台机器有外网, 如果分布的任务到没有外网的机器上就不能执行, 就需要指定本地模式。外网的任务建议使用datax引擎")

    args = parse.parse_args()
    # print parse
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
        "db_name": "",
        "table_name": "",
        "hive_database": "",
        "hive_table": "",
        "is_overwrite": "",
        "query_sql": "",
        "columns": "",
        "filter": None,
        "max_value": None,
        "map_column_hive": "",
        "hive_partition_key": "",
        "hive_partition_value": "",
        "fields_terminated_by": None,
        "line_terminated_by": None,
        "use_raw_null": "1",
        "use_local_mode": "0",
        "warehouse_dir": "",
        "class_name": None,
        "outdir": None,
        "split_by": None,
        "m": None,
        "is_drop": "0",
        "exec_engine": "sqoop"
    }
    # table_names = []
    # conn_names = []

    wizard_name = args.wizard
    if args.source_table and len(args.source_table.split(".")) != 2:
        print("-s的参数必须是库名加表名，例如:pms.ppets")
        sys.exit(1)
    if args.target_table and len(args.target_table.split(".")) != 2:
        print("-t的参数必须是库名加表名，例如:pet_medical.ods_pmsweb_ppets")
        sys.exit(1)

    db = args.db.lower()
    tb = args.tb.lower()
    args_dict["exec_engine"] = args.exec_engine
    args_dict["db_name"] = args.source_table.split(".")[0].lower() if args.source_table else ""
    args_dict["table_name"] = args.source_table.split(".")[1].lower() if args.source_table else ""
    args_dict["hive_database"] = args.target_table.split(".")[0].lower() if args.target_table else ""
    args_dict["hive_table"] = args.target_table.split(".")[1].lower() if args.target_table else ""
    args_dict["m"] = args.num_pappers
    args_dict["is_overwrite"] = "1" if args.hive_overwrite else "0"
    args_dict["hive_partition_key"] = args.hive_partition_key
    args_dict["hive_partition_value"] = args.hive_partition_value
    args_dict["use_local_mode"] = "1" if args.use_local_mode else "0"

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
           s.db_name,
           s.table_name,
           s.hive_database,
           s.hive_table,
           s.is_overwrite,
           s.query_sql,
           s.columns,
           s.filter,
           s.max_value,
           s.map_column_hive,
           s.hive_partition_key,
           s.hive_partition_value,
           s.fields_terminated_by,
           s.line_terminated_by,
           s.use_raw_null,
           s.use_local_mode,
           s.warehouse_dir,
           s.class_name,
           s.outdir,
           s.split_by,
           s.m,
           s.is_drop,
           s.exec_engine
      FROM meta_connections t
      LEFT JOIN meta_import s ON t.connection_id = s.connection_id
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
    args_dict["hive_database"] = args_dict["hive_database"]
    args_dict["hive_table"] = args_dict["hive_table"]
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
    if args_dict["hive_partition_value"] == "$yesterday":
        args_dict["hive_partition_value"] = get_yesterday()
    if not is_valid(args_dict["m"]):
        args_dict["m"] = 1
    if args.num_pappers != '1':
        args_dict["m"] = args.num_pappers
    args_dict["m"] = int(args_dict["m"])
    print(args_dict)
    return args_dict


def sqoop_import(row, where_str, map_column_hive_str):
    host = row["host"]
    port = row["port"]
    db_name = row["db_name"]
    jdbc_extend = row["jdbc_extend"]
    username = "--username '{}' \t".format(row["user"])
    password = "--password '{}' \t".format(row["password"])
    table_name = "--table {} \t".format(row["table_name"])
    hive_import = "--hive-import \t"
    hive_overwrite = "--hive-overwrite \t" if row["is_overwrite"] == "1" else ""
    hive_table = "--hive-table {} \t".format(row["hive_database"] + "." + row["hive_table"])
    # create_hcatalog_table = "--create-hcatalog-table \t" if is_exists != 1 else ""
    # hcatalog_database = "--hcatalog-database {} \t".format(row["hive_database"])
    # hcatalog_table = "--hcatalog-table {} \t".format(row["hive_table"])
    # hcatalog_storage_stanza = "--hcatalog-storage-stanza 'stored as orc' \t"
    # columns = "--columns '{}' \t".format(row["columns"]) if is_valid(row["columns"]) else ""
    # map_column_hive = " --map-column-hive " + row["map_column_hive"] if is_valid(row["map_column_hive"]) else ""
    map_column_hive = "--map-column-hive '{}' \t".format(map_column_hive_str) if len(map_column_hive_str) > 0 else ""
    hive_partition_key = "--hive-partition-key '{}' \t".format(row["hive_partition_key"]) if is_valid(row["hive_partition_key"]) else ""
    hive_partition_value = "--hive-partition-value '{}' \t".format(row["hive_partition_value"]) if is_valid(row["hive_partition_value"]) else ""
    fields_terminated_by = "--fields-terminated-by '{}' \t".format(row["fields_terminated_by"]) if is_valid(row["fields_terminated_by"]) else ""
    line_terminated_by = "--lines-terminated-by '{}' \t".format(row["line_terminated_by"]) if is_valid(row["line_terminated_by"]) else ""
    hive_drop_import_delims = "--hive-drop-import-delims \t"
    use_raw_null = "--null-string '\\\\N' --null-non-string '\\\\N' \t" if row["use_raw_null"] == "1" else ""
    use_local_mode = " -jt local \t" if row["use_local_mode"] == "1" else " \t"
    warehouse_dir = "--warehouse-dir /tmp/`whoami`/{} \t".format(row["warehouse_dir"]) if row["warehouse_dir"] else ""
    class_name = "–-class-name '{}' \t".format(row["class_name"]) if is_valid(row["class_name"]) else ""
    outdir = "--outdir '{}' \t".format(row["outdir"]) if is_valid(row["outdir"]) else ""
    split_by = "--split-by '{}' \t" + row["split_by"] if is_valid(row["split_by"]) else ""
    m = "-m {}".format(row["m"]) if is_valid(row["m"]) else "-m 1"
    # use_direct = " --direct" if is_valid(row["use_direct"]) else ""
    use_direct = ""
    is_drop = row["is_drop"]
    columns = ""  # row["columns"]

    db_type = row["db_type"].lower()
    if db_type == "mysql":
        connect = "--connect 'jdbc:mysql://%s:%s/%s?%s' \t" % (host, port, db_name, jdbc_extend)
        # jdbc:mysql://10.15.1.11:3306/pms?useUnicode=true&characterEncoding=utf-8
    elif db_type == "sqlserver":
        connect = "--connect 'jdbc:sqlserver://%s:%s;database=%s' \t" % (host, port, db_name)
        # jdbc:sqlserver://10.15.1.11:2121;database=PMS
    elif db_type == "oracle":
        connect = "--connect 'jdbc:oracle:thin:@%s:%s:%s' \t" % (host, port, db_name)
        # jdbc:oracle:thin:@192.168.0.147:1521:ORCL
    else:
        raise Exception("ERROR: THE DATABASE TYPE IS NULL OR %s NOT SUPPORT ." % row["db_type"])

    # 3、sqoop语句执行
    sqoop_cmd = "\n        sqoop import" + \
                use_local_mode + \
                connect + \
                username + \
                password + \
                table_name + \
                use_direct + \
                hive_import + \
                hive_overwrite + \
                hive_table + \
                columns + \
                where_str + \
                hive_partition_key + \
                hive_partition_value + \
                map_column_hive + \
                fields_terminated_by + \
                line_terminated_by + \
                hive_drop_import_delims + \
                use_raw_null + \
                warehouse_dir + \
                class_name + \
                outdir + \
                split_by + \
                m
    # sqoop_cmd = "\n        sqoop import" + use_local_mode + connect + username + password + table_name + use_direct + hcatalog_database + hcatalog_table + columns + where + hive_partition_key + hive_partition_value + map_column_hive + fields_terminated_by + line_terminated_by + hive_drop_import_delims + use_raw_null + class_name + outdir + split_by + create_hcatalog_table + hcatalog_storage_stanza + m
    sqoop_cmd = '\\\n'.join(["        " + x for x in sqoop_cmd.split("\t")])
    Dlogger.info("Shell Command = " + sqoop_cmd)
    subprocess.check_output(sqoop_cmd, shell=True)


def datax_generate_json(row, where_str, hive_cols):
    # print(str(hive_cols))
    host = row["host"]
    port = row["port"]
    database = row["db_name"]

    if is_valid(row["query_sql"]):
        query_sql = ' "querySql": ["{};"], '.format(row["query_sql"])
        table = ""
    else:
        query_sql = ""
        table = '"table": ["{}"],'.format(row["table_name"])

    if is_valid(row["columns"]):
        columns = ",".join(['"' + x + '"' for x in row["columns"].strip().split(",")])
        columns = "[" + columns + "]"
    else:
        columns = '["*"]'

    if is_valid(row["hive_partition_key"]) and is_valid(row["hive_partition_value"]):
        hive_partition_str = "/{}={}".format(row["hive_partition_key"], row["hive_partition_value"])
    else:
        hive_partition_str = ""

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
                    "name": "%s",
                    "parameter": {
                        "username": "%s",
                        "password": "%s",
                        "column": %s,
                        "connection": [
                            {   
                                "jdbcUrl": ["%s"],
                                %s%s
                            }
                        ],
                        "where": "%s"
                    }
                },
                "writer": {
                    "name": "hdfswriter",
                    "parameter": {
                        "defaultFS": "%s",
                        "path": "/user/hive/warehouse/%s.db/%s%s",
                        "column": %s,
                        "fileName": "%s",
                        "fileType": "text",
                        "fieldDelimiter": "\u0001",
                        "writeMode": "append"
                    }
                }
            }
        ]
    }
}
    """ % (row["m"], row["db_type"] + "reader", row["user"], row["password"], columns, jdbc_driver, query_sql, table, where_str, config.HDFS, row["hive_database"], row["hive_table"], hive_partition_str, str(hive_cols), row["hive_table"])
    with open(os.path.join(config.PROJECT_PATH, "conf/datax_json/{}.json".format(row["hive_full_name"])), "w") as f:
        f.write(template_json)
    Dlogger.info("generate datax json succeed")


def datax_import(row, where_str, hive_cols):
    datax_generate_json(row, where_str, hive_cols)
    datax_cmd = """{python2} {datax} {template_json}""" \
        .format(python2=config.PYTHON2,
                datax=config.DATAX,
                template_json=os.path.join(config.PROJECT_PATH, "conf/datax_json/{}.json".format(row["hive_full_name"]))
                )
    Dlogger.info("Shell Command = " + datax_cmd)
    # subprocess.check_output(datax_cmd, shell=True)
    subprocess.check_call(datax_cmd, shell=True)


def prepare_ddl(exec_engine, hive_full_name, cols_list_map, hive_cols, hive_partition_key, hive_partition_value, alter_sql, is_overwrite):
    # 表结构语句判断
    schema_sql = ""
    if len(hive_cols) == 0:
        if exec_engine == "sqoop":
            # sqoop使用hive-import模式无法讲数据插入到orc格式中, 只能使用hcatalog模式, 但是这个模式的分区有权限问题
            # 使用hive-import模式, 就不需要建表语句了
            schema_sql = ""
        else:  # exec_engine=="datax"
            if is_valid(hive_partition_key) and is_valid(hive_partition_value):
                part_sql = " partitioned by ({} string)".format(hive_partition_key)
            else:
                part_sql = ""
            # orc不能repalce字段，而且便于表结构一样就可以sqoop和datax任意切换，所以不用orc格式，使用sqoop默认的text格式
            # schema_sql = """create table if not exists {table} (\n{cols}\n        )\n{part_sql}        stored as orc;""" \
            schema_sql = """create table if not exists {table} (\n{cols}\n        ){part_sql}""" \
                .format(table=hive_full_name,
                        cols=''.join(["            {name} {type},\n".format(name=x["name"], type=x["type"]) for x in cols_list_map])[:-2],
                        part_sql=part_sql) \
                .lower()

    else:
        if len(alter_sql) > 0:
            schema_sql = "".join(alter_sql)

    # 删除数据判断
    delete_data_sql = ""
    if exec_engine == "datax" and is_overwrite == "1":
        if hive_partition_key and hive_partition_key:
            delete_data_sql = """
        alter table {hive_full_table} drop if exists partition ({hive_partition_key}='{hive_partition_value}')""". \
                format(hive_full_table=hive_full_name,
                       hive_partition_key=hive_partition_key,
                       hive_partition_value=hive_partition_value)
        else:
            delete_data_sql = """
        truncate table {hive_full_table}""". \
                format(hive_full_table=hive_full_name,
                       hive_partition_key=hive_partition_key,
                       hive_partition_value=hive_partition_value)

    # 分区语句判断
    add_partition_sql = ""
    if exec_engine == "datax" and is_valid(hive_partition_key) and is_valid(hive_partition_value):
        add_partition_sql = """
        alter table {hive_full_table} add if not exists partition ({hive_partition_key}='{hive_partition_value}')""" \
            .format(hive_full_table=hive_full_name,
                    hive_partition_key=hive_partition_key,
                    hive_partition_value=hive_partition_value)

    # 执行语句
    if schema_sql + delete_data_sql + add_partition_sql != "":
        engine_str = get_engine_str("hive").format(**config.HIVE_CONF)
        con = create_engine(engine_str, poolclass=pool.NullPool)
        if schema_sql != "":
            for tmp_schema_sql in [x for x in schema_sql.split(";") if x]:
                Dlogger.info("\n        " + tmp_schema_sql + ";")
                con.execute(tmp_schema_sql)
        if delete_data_sql != "":
            for tmp_drop_partition_sql in [x for x in delete_data_sql.split(";") if x]:
                Dlogger.info(tmp_drop_partition_sql + ";")
                con.execute(tmp_drop_partition_sql)
        if add_partition_sql != "":
            for tmp_add_partition_sql in [x for x in add_partition_sql.split(";") if x]:
                Dlogger.info(tmp_add_partition_sql + ";")
                con.execute(tmp_add_partition_sql)


def prepare_increase(row):
    max_sql = "select {max_value} from {hive_full_name} limit 1".format(max_value=row["max_value"], hive_full_name=row["hive_full_name"])
    engine_str = get_engine_str("hive").format(**config.HIVE_CONF)
    con = create_engine(engine_str, poolclass=pool.NullPool)
    Dlogger.info(max_sql)
    try:
        df = pd.read_sql(sql=max_sql, con=con)
        max_value = df.iat[0, 0] if not df.empty else 0
    except Exception as e:
        Dlogger.warn("{} Not Exists Or Table Name Error".format(row["hive_full_name"]))
        max_value = 0
    Dlogger.info("Max Value : {}".format(max_value))
    if row["exec_engine"] == "sqoop":
        where_str = '--where "{}" \t'.format(row["filter"])
    else:
        where_str = row["filter"]
    where_str = where_str.format(max_value=max_value)
    return where_str

    # if is_valid(row["max_value"]) and is_valid(row["filter"]):
    #     where = '--where "{}" \t'.format(row["filter"])
    #     max_sql = """
    #     hive -e "
    #     set hive.mapred.mode=nonstrict;
    #     select {max_value} from {hive_full_name} limit 1;
    #     " """.format(max_value=row["max_value"], hive_full_name=hive_full_name)
    # elif not is_valid(row["max_value"]) and not is_valid(row["filter"]):
    #     where = ""
    #     max_sql = ""
    # else:
    #     raise Exception("ERROR: %s的filter和max_value必须同时填写或着都不填写, 请仔细核实。" % (row["hive_table"]))
    #
    # # 2、最大值sql语句判断和执行
    # if max_sql != "":
    #     if len(hive_cols) == 0:
    #         max_value = 0
    #     else:
    #         Dlogger.info("Shell Command = " + max_sql)
    #         max_value = subprocess.check_output(max_sql, shell=True)
    #         max_value = max_value.strip()
    #     Dlogger.info("Max Value : {}".format(max_value))
    #     where = where.format(max_value=max_value)


def main(row):
    # 1、建表语句判断和执行
    cols_list_map, hive_cols, map_column_hive, alter_sql = schema_check.check_db_to_hive(row["table_name"], row["hive_full_name"], row["db_conf"], row["db_type"])
    # print(cols_list_map)

    # 过滤指定的字段，用指定的字段创建表结构，主要是ehr的sqlserver那个数据库
    # 暂时没做columns和query_sql的冲突，判断校验
    if is_valid(row["columns"]):
        columns_tmp = row["columns"].strip().lower().split(",")
        cols_list_map = [x for x in cols_list_map if x["name"].lower() in columns_tmp]
    if is_valid(row["query_sql"]):
        # columns_tmp = row["query_sql"].strip().lower()
        columns_tmp = re.sub("select | from.*", "", row["query_sql"].strip().lower()).split(",")
        cols_list_map_tmp = []
        for column_name in columns_tmp:
            # 需要固定字段顺序
            cols_list_map_tmp.append([x for x in cols_list_map if x["name"].lower() == column_name][0])
        cols_list_map = cols_list_map_tmp
        # print(cols_list_map)
    prepare_ddl(row["exec_engine"], row["hive_full_name"], cols_list_map, hive_cols, row["hive_partition_key"], row["hive_partition_value"], alter_sql, row["is_overwrite"])

    # 2、最大值sql语句判断和执行
    if len(hive_cols) > 0 and is_valid(row["max_value"]) and is_valid(row["filter"]):
        where_str = prepare_increase(row)
    else:
        if row["exec_engine"] == "sqoop":
            where_str = '--where "1=1" \t'
        else:
            where_str = "1=1"

    # 3、import data
    if row["exec_engine"] == "sqoop":
        sqoop_import(row, where_str, map_column_hive)
    else:  # exec_engine == "datax"
        if len(hive_cols) == 0 or len(alter_sql) != 0:
            hive_cols = cols_list_map
        datax_import(row, where_str, hive_cols)
    drop_duplicates(row["hive_full_name"], row["hive_partition_key"])


if __name__ == "__main__":
    args_dict = pre_args()
    # 开始执行
    start_time = datetime.datetime.now()
    main(args_dict)
    end_time = datetime.datetime.now()
    Dlogger.info("Total Time Taken: {} seconds".format(str((end_time - start_time).seconds)))
