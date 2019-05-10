#!/usr/bin/env python
# coding=utf-8

import argparse
import sys
import re
import subprocess
import pandas as pd
from sqlalchemy import create_engine, pool
from conf import config
from utility import get_engine_str, is_valid, get_yesterday

Dlogger = config.get_logger("DataUnique")


def is_increase(hive_full_table):
    sql = """select filter,max_value from meta_import where lower(hive_database)=lower('{hive_database}') and lower(hive_table)=lower('{hive_table}') limit 1;""" \
        .format(hive_database=hive_full_table.split(".")[0],
                hive_table=hive_full_table.split(".")[1])
    engine_str = get_engine_str("mysql").format(**config.DB_CONF)
    con = create_engine(engine_str, poolclass=pool.NullPool)
    Dlogger.info(sql)
    # rows=con.execute(sql)
    df = pd.read_sql(sql=sql, con=con)
    if not df.empty:
        filter = df.iat[0, 0]
        max_value = df.iat[0, 1]
        if is_valid(filter) and is_valid(max_value):
            return True
    return False


def drop_duplicates(hive_full_table, partition):
    if "warm_deleted" in hive_full_table:
        Dlogger.warn("{} skip drop duplicates".format(hive_full_table))
        return

    source_tb = hive_full_table
    target_tb = hive_full_table.replace("src", "ods")
    if "pet_medical.src_" in source_tb:
        if re.search("_pms_|_pmsweb_", source_tb, re.I) and is_increase(source_tb) and not re.search("operationlogdetail", source_tb, re.I):
            # if ("pcustomers","ppets","pclientmanagement") in hive_table:
            if "pet_medical.src_pms_" in source_tb:
                tablename = source_tb.replace("pet_medical.src_pms_", "")
                del_table = "pet_medical.src_pms_warm_deleted"
            else:
                tablename = source_tb.replace("pet_medical.src_pmsweb_", "")
                del_table = "pet_medical.src_pmsweb_warm_deleted"

            src_id = "id"
            src_updatestamp = "updatestamp"

            if re.search("sysicd10", source_tb, re.I):
                src_id = "icdcode"
            if re.search("pcustomers|ppets|operationlog|operationlogdetail", source_tb, re.I):
                src_updatestamp = "updatetimestamp"

            sql = """
            set hive.support.quoted.identifiers=none;
            set hive.mapred.mode=nonstrict;
            set hive.execution.engine=mr;
            
            drop table {target_tb};
            create table {target_tb} stored as orc as
            select \`(rank_tmp|p_id|p_orgid|p_updatestamp)?+.+\`
              from (
                    select a.*,current_timestamp() as etl_time
                     from (
                            select *,row_number() over(partition by {src_id},orgid order by {src_updatestamp} desc) rank_tmp
                              from {source_tb}
                          ) a where a.rank_tmp=1
                   ) a
            left join
                   (select p_id,p_orgid,p_updatestamp
                      from
                         (
                           select p_id,p_orgid,updatestamp as p_updatestamp,row_number() over(partition by p_id,p_orgid order by updatestamp desc) as rank_tmp
                             from {del_table} 
                            where lower(tablename)=lower('{tablename}')
                         ) b where b.rank_tmp=1
                   ) b on a.{src_id}=b.p_id and a.orgid=b.p_orgid
            where b.p_updatestamp is null or a.{src_updatestamp}>b.p_updatestamp;
            """.format(target_tb=target_tb,
                       source_tb=source_tb,
                       tablename=tablename,
                       del_table=del_table,
                       src_id=src_id,
                       src_updatestamp=src_updatestamp)
        elif re.search("_his_", source_tb, re.I) and is_increase(source_tb):
            sql = """
            set hive.support.quoted.identifiers=none;
            set hive.mapred.mode=nonstrict;
            set hive.execution.engine=mr;
            
            drop table {target_tb};
            create table {target_tb} stored as orc as
            select \`(rank_tmp)?+.+\`
              from (
                    select *,current_timestamp() as etl_time,row_number() over(partition by id,orgid order by update_time desc) as rank_tmp 
                      from {source_tb}
                   ) t
             where t.rank_tmp=1;
            """.format(target_tb=target_tb,
                       source_tb=source_tb)
        elif re.search("_chongdun_|_lianchong_|_yichong_|_xunde_|_vetbuddy_", source_tb, re.I) and "clinic_payment_book" not in source_tb:
            if "crm_customer" in source_tb:
                tmp_col = "t.cus_logicid"
            elif "crm_pet" in source_tb:
                tmp_col = "t.pet_logicid"
            elif "clinic_case_regonly" in source_tb:
                tmp_col = "t.regid"
            elif "clinic_paymentdetails_book" in source_tb:
                tmp_col = "t.id,t.paymentid"
            # elif "clinic_payment_book" in source_tb:    # 如果是会员卡充值等，该表的paymentid可能就是空的
            #     tmp_col = "t.paymentid"
            else:  # clinic_product
                tmp_col = "t.id"

            sql = """
            set hive.support.quoted.identifiers=none;
            set hive.mapred.mode=nonstrict;
            
            drop table {target_tb};
            create table {target_tb} stored as orc as
            select \`(rank_tmp)?+.+\` 
              from (select t.*,current_timestamp() as etl_time,row_number() over(partition by {tmp_col},t.orgid order by t.updatetime desc) rank_tmp from {source_tb} t) t
             where t.rank_tmp=1;
            """.format(target_tb=target_tb,
                       source_tb=source_tb,
                       tmp_col=tmp_col)
        else:
            if partition:
                where_str = " where {partition}='{date}'".format(partition=partition, date=get_yesterday())
            else:
                where_str = ""
            sql = """
            drop table {target_tb};
            create table {target_tb} stored as orc as 
            select *,current_timestamp() as etl_time from {source_tb}{where_str};
            """.format(target_tb=target_tb,
                       source_tb=source_tb,
                       where_str=where_str)
        # 日志不详细，还是选择使用shell命令行
        # engine_str = get_engine_str("hive").format(**config.HIVE_CONF)
        # con = create_engine(engine_str, poolclass=pool.NullPool, echo=True)
        # Dlogger.info(sql)
        # for tmp_sql in [x for x in sql.split(";") if x.strip()]:
        #     con.execute(tmp_sql)
        Dlogger.info(sql)
        shell_cmd = """ hive -e "{sql}" """.format(sql=sql)
        subprocess.check_call(shell_cmd, shell=True)
    else:
        Dlogger.error("DataUnique: Hive Table Name Error")


def pre_args():
    parse = argparse.ArgumentParser(prog="DataUnique", description="I am help message...")
    parse.add_argument("-t", "--hive_table", help="Hive Full Table Name, Example: pet_medical.src_pms_PPets")
    parse.add_argument("-p", "--partition", help="分区字段")
    parse.add_argument("--auto", action="store_true", help="执行全部任务, 未完成")
    args = parse.parse_args()
    print(args)
    if not args.hive_table and not args.auto:
        print("必须指定参数, 输入--help查看相应参数")
        sys.exit(1)
    if "." not in args.hive_table:
        print("输入的表名有误, 必须是完整的名称, 比如: pet_medical.src_pms_PPets")
        sys.exit(1)
    return args


if __name__ == "__main__":
    args = pre_args()
    drop_duplicates(args.hive_table, args.partition)
