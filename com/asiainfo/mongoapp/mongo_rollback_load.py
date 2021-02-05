import datetime
import logging
import os
import re
import socket

import mongo_writer

logging.basicConfig(level=logging.DEBUG)


def test():
    mongo_data_dir = '/Users/mtr/PycharmProjects/mongoQuery/resource/mongo_data'
    rollback_dir = 'rollback'
    for root, dirs, files in os.walk(mongo_data_dir):
        print('root_dir:', root)
        # print('sub_dirs:', dirs)
        if not str(root).__contains__(rollback_dir):
            continue
        for file in files:
            full_path_of_file = os.path.join(root, file)
            logging.debug(f'full_path_of_file: {full_path_of_file}')
    excep_pattern = re.compile(r'.*\.bson$')
    logging.debug(excep_pattern)
    # file_name1 = '851-9072407196067231316.wt'
    file_name = 'Store0.dr_gsm_0_0_20210203.bson'
    # Store0.dr_gsm_0_0_20210203.bson
    result = excep_pattern.findall(file_name)
    logging.info(f"result is {result}")
    # date_pattern = re.compile(r'(\d{4}-\d{1,2}-\d{1,2}\s\d{1,2}:\d{1,2}:\d{1,2}[,|.]\d+)')
    # date = date_pattern.findall(file_name)
    # print(date)


# 获取 path_of_mongo_data 下含目录名为rollback_dir的bson文件 即rollback文件
def get_rollback_file_info(path_of_mongo_data, rollback_dir, pattern):
    rollback_files_dic = {}
    for root, dirs, files in os.walk(path_of_mongo_data):
        if not str(root).__contains__(rollback_dir):
            continue
        logging.debug(f'files is: {files}')
        for file in files:
            full_path_of_file = os.path.join(root, file)
            if not pattern.match(full_path_of_file):
                continue
            file_modify_time_from_path = os.path.getmtime(full_path_of_file)
            logging.debug(f'root is {root}, file is {file} , full_path_of_file is : {full_path_of_file}')
            rollback_files_dic.update({full_path_of_file: file_modify_time_from_path})
    return rollback_files_dic


# 获取前before_day的日期
def get_date(before_day):
    today = datetime.datetime.now()
    offset = datetime.timedelta(days=-before_day)
    date = (today + offset).strftime('%Y%m%d')
    logging.info(f'day is {date}')
    return date


# insert stat
def insert_stat(mongo_client, stat_db_name, stat_coll_name, filename, file_modify_time_from_path):
    start_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
    status = 'start'
    line_number = '-1'
    condition = {"FileName": filename}
    doc = {"$set": {'StartTime': start_time, 'Status': status, 'FileLineNumber': line_number,
                    'FileName': filename, "FileModifyTime": file_modify_time_from_path}}
    writer.conn_update(mongo_client, stat_db_name, stat_coll_name, condition, doc, True)


# update stat
def update_stat(mongo_client, stat_db_name, stat_coll_name, filename, line_number, file_modify_time_from_path):
    end_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
    status = 'finish'
    condition = {"FileName": filename}
    doc = {"$set": {'Status': status, 'FileLineNumber': line_number, 'EndTime': end_time,
                    "FileModifyTime": file_modify_time_from_path}}
    logging.info(f'conditon is: {condition}, doc is : {doc}')
    writer.conn_update(mongo_client, stat_db_name, stat_coll_name, condition, doc, True)


def combine_rollback_stat(db_client, db, coll, function_name, name):
    stat_dic = dict({"FileName": name,
                     "InputTime": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'),
                     "FunctionName": function_name,
                     "IpAddr": get_ip()})
    mongo_writer.conn_insertone(db_client, db, coll, stat_dic)


# 查询stat入库的任务stat信息,和filepath路径下文件比对，获取需要增量处理的stat文件名和行号
def get_deal_file_dic(mongo_client, stat_db_name, stat_coll_name, file_info_dic):
    condition = {'Status': 'finish'}
    result_list = mongo_writer.conn_query(mongo_client, stat_db_name, stat_coll_name, condition)
    logging.info(f'result_list is {result_list}')
    if result_list is not None:
        for doc in result_list:
            file_name = doc.get('FileName')
            logging.debug(f'filename is {file_name}')
            if file_name is not None:
                file_info_dic.pop(file_name)
    logging.info(f'file_deal_dic is {file_info_dic}')
    return file_info_dic


# 获取计算机名称
def get_ip():
    hostname = socket.gethostname()
    # 获取本机IP
    ip = socket.gethostbyname(hostname)
    logging.debug(ip)
    return ip


if __name__ == '__main__':
    # 数据库认证
    username = 'root'
    password = 'root'
    mongos_host = '10.19.85.33'
    mongos_port = 34000
    db_name = 'test'
    coll_name = 'stat_rollback_20210129'
    writer = mongo_writer
    client = writer.auth(username, password, mongos_host, mongos_port)
    stat_db = "test"
    stat_coll = "mongo_rollback_load_stat_" + get_date(0)
    mongo_data_dir_name = '/Users/mtr/PycharmProjects/mongoQuery/resource/mongo_data'
    rollback_dir_name = 'rollback'
    bson_pattern = re.compile(r'.*\.bson$')
    func_name = 'rollback'
    file_info_dic_from_path = get_rollback_file_info(mongo_data_dir_name, rollback_dir_name, bson_pattern)
    file_dic = get_deal_file_dic(client, stat_db, stat_coll, file_info_dic_from_path)
    logging.info(f'files_dic is {file_dic}')
    for name in file_dic.keys():
        file_modify_time = file_info_dic_from_path.get(name)
        insert_stat(client, stat_db, stat_coll, name, file_modify_time)
        combine_rollback_stat(client, db_name, coll_name, func_name, name)
        update_stat(client, stat_db, stat_coll, name, 1, file_modify_time)
    print('-----')
    get_ip()
