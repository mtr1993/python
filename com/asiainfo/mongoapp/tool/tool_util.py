import logging
import os
import datetime


# 获取前before_day的日期
import socket
from itertools import islice

from asiainfo.mongoapp.mongo import mongo_writer
# logging.basicConfig(level=logging.INFO,
#                     filename='/Users/mtr/PycharmProjects/mongoQuery/resource/log/tool_util.log',
#                     filemode='a',
#                     format='%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s')


def get_date(before_day):
    today = datetime.datetime.now()
    offset = datetime.timedelta(days=-before_day)
    date = (today + offset).strftime('%Y%m%d')
    logging.info(f'day is {date}')
    return date


# 获取对应目录下文件和最后修改时间
def get_file_info(file_path, file_regex):
    file_info_dic = {}
    for file_name in os.listdir(file_path):
        if not file_name.__contains__(file_regex):
            continue
        full_name = os.path.join(file_path, file_name)
        logging.debug(f' full_name is {full_name}')
        if os.path.isfile(full_name):
            file_info_dic[full_name] = os.path.getmtime(full_name)
    logging.info(f' file_info_dic is {file_info_dic}')
    return file_info_dic


# 读取stat文件中信息
def read_stat_info(file_name, line_index):
    stat_list = []
    with open(file_name, 'r', encoding='utf-8', errors='ignore') as f_input:
        for stat_line_str in islice(f_input, line_index + 1, None):
            stat_line_str = stat_line_str.strip('\n')
            stat_list.append(stat_line_str)
            logging.info(f'stat_line_str is: {stat_line_str}')

    return stat_list


# 查询stat入库的任务stat信息,和filepath路径下文件比对，获取需要增量处理的stat文件名和行号
def get_deal_file_dic(mongo_client, stat_db_name, stat_coll_name, file_info_dic):
    condition = {'Status': 'finish'}
    file_deal_dic = init_file_deal_dic(file_info_dic)
    result_list = mongo_writer.conn_query(mongo_client, stat_db_name, stat_coll_name, condition)
    logging.info(f'result_list is {result_list}')
    if result_list is not None:
        for doc in result_list:
            file_name = doc.get('FileName')
            file_modify_time_from_stat = doc.get("FileModifyTime")
            file_line_number = doc.get("FileLineNumber")
            file_modify_time_from_path = file_info_dic.get(file_name)
            logging.debug(f'filename is {file_name}, file_modify_time_from_stat is {file_modify_time_from_stat} '
                          f', file_modify_time_from_path  is {file_modify_time_from_path}'
                          f'， file_line_number is {file_line_number}')
            if file_modify_time_from_stat is not None and file_modify_time_from_stat == file_modify_time_from_path:
                file_deal_dic.pop(file_name)
                continue
            else:
                file_deal_dic.update({file_name: file_line_number})
    logging.info(f'file_deal_dic is {file_deal_dic}')
    return file_deal_dic


# 初始化需要处理的所有文件 key = 文件名 value = -1 文件行  文件名来自filepath下
def init_file_deal_dic(file_info_from_path: dict):
    file_deal_dic = {}
    for key in file_info_from_path.keys():
        file_deal_dic.setdefault(key, -1)
    logging.debug(f'file_deal_dic is: {file_deal_dic}')
    return file_deal_dic


# 获取计算机名称
def get_ip():
    hostname = socket.gethostname()
    # 获取本机IP
    ip = socket.gethostbyname(hostname)
    logging.debug(ip)
    return ip
