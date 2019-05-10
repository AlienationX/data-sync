#!/usr/bin/env python
# coding=utf-8

""" 选择shell执行日志更加详细 """

import re
import datetime
import subprocess
import pandas as pd
import argparse
from sqlalchemy import create_engine, pool
from conf import config
from utility import get_engine_str

Dlogger = config.get_logger("DataMerge")


def merge_data(pms_table):
    clinic_table = "pet_medical.mir_clinic"

    # HIS诊疗模块没有的表，老小暖的诊疗相关表数据
    if re.search("CEMRecord|CPrescriptions|CPrescriptionDetails|CPhysical|CEMRecordPhysical|CLabReport|CLabVirusDetails|PBCheckList|PXRaysList|PPathologyList", pms_table, re.I):
        if "cemrecord" in pms_table.lower():
            file_type = "rcfile"
        else:
            file_type = "orc"
        sql = """
        drop table {data_xiaonuan_final_table};
        create table {data_xiaonuan_final_table} stored as {file_type} as
        select * from {pms_table};
        """
        exec_sql = sql.format(pms_table=pms_table, file_type=file_type, data_xiaonuan_final_table=pms_table.replace("pet_medical.ods_pms_", "data_xiaonuan_final."))
    else:
        pmsweb_table = pms_table.replace("ods_pms_", "ods_pmsweb_")
        mid_merge_table = pms_table.replace("ods_pms_", "mid_")
        sql1 = "desc {pms_table}".format(pms_table=pms_table)
        sql2 = "desc {pmsweb_table}".format(pmsweb_table=pmsweb_table)
        engine_str = get_engine_str("hive").format(**config.HIVE_CONF)
        con = create_engine(engine_str, poolclass=pool.NullPool)
        Dlogger.info(sql1)
        df1 = pd.read_sql(sql=sql1, con=con)
        Dlogger.info(sql2)
        df2 = pd.read_sql(sql=sql2, con=con)
        # print(df1)
        # print(df2)
        same_df = pd.merge(df1, df2, on="col_name", how="inner")["col_name"]
        same_columns = same_df.tolist()
        columns_str = ",".join(["t." + x for x in same_columns])
        if re.search("SysCategory", pms_table, re.I):  # SysCategory以pmsweb为主吧，其实都差不多
            sql = """
            drop table {data_xiaonuan_final_table};
            create table {data_xiaonuan_final_table} stored as orc as
            select * from {pmsweb_table};
            """
        elif re.search("PClientManagement", pms_table, re.I):
            sql = """
            drop table {mid_merge_table};
            create table {mid_merge_table} stored as orc as 
            select {columns_str} from {pms_table} t left join {clinic_table} s on t.id=s.clinic_id where s.pmsweb_id is null
            union all
            select {columns_str} from {pmsweb_table} t left join {clinic_table} s on t.id=s.clinic_id where s.pmsweb_id is not null;
            
            drop table {mir_merge_table};
            create table {mir_merge_table} stored as orc as
            select s.brand_code,s.clinic_name,t.* from {mid_merge_table} t join (select brand_code,clinic_id,clinic_name from {clinic_table}) s on t.id=s.clinic_id;
            
            drop table {data_xiaonuan_final_table};
            create table {data_xiaonuan_final_table} stored as orc as
            select * from {mir_merge_table};
            """
        else:
            sql = """
            drop table {mid_merge_table};
            create table {mid_merge_table} stored as orc as 
            select {columns_str} from {pms_table} t left join {clinic_table} s on t.orgid=s.clinic_id where s.pmsweb_id is null
            union all
            select {columns_str} from {pmsweb_table} t left join {clinic_table} s on t.orgid=s.clinic_id where s.pmsweb_id is not null;
            
            drop table {mir_merge_table};
            create table {mir_merge_table} stored as orc as
            select s.brand_code,s.clinic_name,t.* from {mid_merge_table} t join (select brand_code,clinic_id,clinic_name from {clinic_table}) s on t.orgid=s.clinic_id;
    
            drop table {data_xiaonuan_final_table};
            create table {data_xiaonuan_final_table} stored as orc as
            select * from {mir_merge_table};
            """
        exec_sql = sql.format(mid_merge_table=mid_merge_table,
                              columns_str=columns_str,
                              pms_table=pms_table,
                              pmsweb_table=pmsweb_table,
                              mir_merge_table=mid_merge_table.replace("mid_", "mir_"),
                              clinic_table=clinic_table,
                              data_xiaonuan_final_table=pms_table.replace("pet_medical.ods_pms_", "data_xiaonuan_final.")
                              )
    # for sql in exec_sql.split(";"):
    #     Dlogger.info(sql)
    #     con.execute(sql)
    Dlogger.info(exec_sql)
    sh_cmd = 'hive -e "{exec_sql}"'.format(exec_sql=exec_sql)
    subprocess.check_output(sh_cmd, shell=True)


def tb_name_verify(tb):
    if "ods_pms_" not in tb:
        raise Exception("{tb} 表名不符合merge的规则, 表名必须包含ods_pms字符, 请验证后重试.".format(tb=tb))
    if "." not in tb:
        raise Exception("{tb} 输入的表名有误, 必须是完整的库名加表名, 比如: pet_medical.ods_pms_PPets".format(tb=tb))


def pre_args():
    parse = argparse.ArgumentParser(prog="DataMerge", description="I am help message...")
    parse.add_argument("-t", "--table_name", required=True, help="Hive Full Table Name, Example: pet_medical.ods_pms_CMembersCard")
    args = parse.parse_args()
    print(args)
    tb_name_verify(args.table_name)
    return args


if __name__ == "__main__":
    args_dict = pre_args()
    # 开始执行
    start_time = datetime.datetime.now()
    merge_data(args_dict.table_name)
    end_time = datetime.datetime.now()
    Dlogger.info("Total Time Taken: {} seconds".format(str((end_time - start_time).seconds)))

