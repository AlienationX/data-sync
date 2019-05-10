#!/usr/bin/env python
# coding=utf-8

import datetime
import pandas as pd
from sqlalchemy import create_engine, pool
from utility import get_engine_str
from conf import config

Dlogger = config.get_logger("DataRowCount")
mysql_engine_str = get_engine_str("mysql").format(**config.DB_CONF)
mysql_con = create_engine(mysql_engine_str, poolclass=pool.NullPool)


def update_rowcount(row):
    sql = "select count(1) as cnt from {table_name}".format(table_name=row["table_name"])
    db_conf = {
        "host": row["host"],
        "port": row["port"],
        "user": row["user"],
        "password": row["password"],
        "database": row["db_name"],
        "charset": "utf8"
    }
    engine_str = get_engine_str(row["db_type"]).format(**db_conf)
    con = create_engine(engine_str, poolclass=pool.NullPool)
    # Dlogger.debug(sql)
    try:
        result = pd.read_sql(sql, con)
        table_rows = result.iat[0, 0]
        sql_update = "update meta_import set table_rows={table_rows},rows_updatetime='{now}' where db_name='{db_name}' and table_name='{table_name}'" \
            .format(table_rows=table_rows,
                    now=str(datetime.datetime.now()),
                    db_name=row["db_name"],
                    table_name=row["table_name"])
        mysql_con.execute(sql_update)
    except Exception:
        table_rows = "Null"
    Dlogger.info("{:<40} : {}".format(row["db_name"] + "." + row["table_name"], table_rows))


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
           s.table_name
      FROM meta_connections t
      JOIN meta_import s ON t.connection_id = s.connection_id
     WHERE t.status = '1'
     ORDER BY t.connection_id;
    """
    # rows = mysql_con.execute(sql)
    # for row in rows:
    #     print(row)
    df = pd.read_sql(sql, mysql_con)
    for index, row in df.iterrows():
        update_rowcount(row)

