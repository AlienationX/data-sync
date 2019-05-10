#!/usr/bin/env python
# coding=utf-8

import datetime
import time
import traceback
import sys
import os
import signal
import subprocess
from collections import OrderedDict
from multiprocessing import Pool
from conf import config
from utility import send_mail

TABLE_NAMES = OrderedDict({
    "PClientManagement": "医院信息表",
    "PCustomers": "客户表",
    "PPets": "宠物表",
    "SysPMSUsers": "用户信息表",
    "SysRoles": "用户权限表",
    "PSysList": "医院自定义码表",
    "SysCategory": "系统码表",  # (没有OrgId)
    "SysSettings": "系统设置表",

    "CMembersCard": "会员卡信息",
    "CMembersCardPayments": "会员卡流水表",
    "CChargeHistories": "账户流水表",
    "CPreChargeHistories": "押金流水表",

    "CMyMoney": "流水表",
    "CPayments": "账单表",
    "CPaymentDetails": "流水明细表",

    "CReg": "挂号表",
    "CEMRecord": "病例表",  # HIS诊疗模块替代
    "CPrescriptions": "处方表",  # HIS诊疗模块替代
    "CPrescriptionDetails": "处方明细表",  # HIS诊疗模块替代
    # "CPhysical": "体格检查--体重体温",  # HIS诊疗模块替代(没有同步，也没有用到，其次数据量700w且没有时间戳无法增量同步)
    "CEMRecordPhysical": "体格检查--检查项目 耳眼口鼻",  # HIS诊疗模块替代
    "CLabReport": "检查化验+试纸检查",  # HIS诊疗模块替代
    "CLabVirusDetails": "试纸检查 报告单详情数据",  # HIS诊疗模块替代
    "PBCheckList": "B超",  # HIS诊疗模块替代
    "PXRaysList": "X光",  # HIS诊疗模块替代
    "PPathologyList": "显微镜",  # HIS诊疗模块替代
    "PLabViursResults": "???",  # pmsweb存在，具体是什么数据还不太清楚，暂时和pms合并

    "PReservation": "预约信息",

    "PMedicines": "产品表",
    "PMedicineRelations": "产品表--设备相关耗材",
    "Manufacturer": "生产商",
    "PProvider": "供应商",
    "Brand": "品牌",
    "IInstoreRecipe": "出入库单据",
    # "IInstoreRecipeHistories":"出入库单据修改记录",      # 修改记录一般数据量大且没用，所以没导
    "IInstoreList": "出入库单据明细",
    # "IInstoreListHistories":"出入库单据明细修改记录",     # 修改记录一般数据量大且没用，所以没导
    "IInstockCheckRecipea": "盘点单据",
    "IInstockCheckList": "盘点单据明细",
    "IInstockRecheckList": "盘点复盘明细",
    "PProductHistories": "入库明细",
    "T_OutStore": "出库明细",
})

# 并行数
_process_nums = 8

# 任务失败重试次数
_retry_count = 3

# 单任务的执行文件
_data_merge_py = os.path.join(sys.path[0], "data_merge.py")

# 日志路径
_log_path = os.path.join(config.LOG_PATH, "data_merge")

if not os.path.exists(_log_path):
    os.makedirs(_log_path)

Dlogger = config.get_logger("DataMergeAuto")


def parallel_write_log(no, tb):
    try:
        table_name = "mid_" + tb.lower()
        input_table_name = "pet_medical.ods_pms_" + tb.lower()
        full_log_path = "{log_path}/data_merge_{table_name}.log".format(log_path=_log_path, table_name=table_name)
        sh_cmd = "{python} -u {data_merge_py} -t {input_table_name} &>> {full_log_path}\n". \
            format(python=config.PYTHON3,
                   data_merge_py=_data_merge_py,
                   input_table_name=input_table_name,
                   full_log_path=full_log_path)
        flag = ""

        for i in range(_retry_count + 1):
            start_time = datetime.datetime.now()
            if i > 0:
                times = "失败任务{}重新执行 第{}次".format(table_name, str(i))
            else:
                times = ""
            print("{no:<3} {table_name:<40}      StartTime: {start_time}      {times}".format(no=no, table_name=table_name, start_time=str(start_time), times=times))
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
            print("{no:<3} {table_name:<40}      StartTime: {start_time}      EndTime: {end_time}      ExecTime: {exec_time:<5}      {flag}".format(no=no, table_name=table_name, start_time=str(start_time), end_time=str(end_time), exec_time=str(exec_time) + "s", flag=flag))
            if flag == "SUCCEEDED":
                break
            else:
                time.sleep(10)

        if flag == "SUCCEEDED":
            return ""
        else:
            return sh_cmd
    except Exception as e:
        print(traceback.format_exc())


def get_all_tasks():
    i = 0
    rank = []
    tb_names = list(TABLE_NAMES.keys())
    tb_names.reverse()  # 倒序执行，大表放在前面
    for key in tb_names:
        i = i + 1
        rank.append({"no": i, "table_name": key})
    return rank


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
        # print(row)
        err_table = p.apply_async(func=parallel_write_log, args=(row["no"], row["table_name"]))
        failed_sh_cmd.append(err_table)
    p.close()
    p.join()
    failed_sh_cmd = [x.get() for x in failed_sh_cmd if x.get() != ""]
    if len(failed_sh_cmd) > 0:
        Dlogger.info("--- Failed Task ---")
        for res in failed_sh_cmd:
            Dlogger.info(res.strip())
        print(" ".join(failed_sh_cmd))
        send_mail("Data Merge Failed", "\n".join(failed_sh_cmd))
    e_times = datetime.datetime.now()
    Dlogger.info("Total Time Taken: {} seconds".format(str((e_times - s_times).seconds)))
