import datetime
import logging
import re

import mongo_writer
import stat_load

logging.basicConfig(level=logging.DEBUG)


def test():
    service_log = '2021-02-02 09:42:35,244 INFO [XdrService] [pool-17-thread-18] ' \
                  'query:13909418992 dr_type:dr_kj mergePlan: start time:20210202 09:42:35:104 ' \
                  'total use:[141ms] count:0 start day:20210201, end day:20210228'
    # replace('start time', 'start_time').replace('total use', 'total_use').\
    # replace('start day', 'start_day').replace('end day', 'end_day')
    # excep_pattern = re.compile('(.+?):(.+?)(\s)(?=|$)')
    service_pattern = re.compile(r'query:.*')
    # excep_pattern = re.compile('(query|dr_type|start time):(.+?)\s(?=|$)')
    # .+?表示最小匹配  \\[(.*?)\\] 获取括号内的内容
    # service_pattern = re.compile('(query):(.+?)\s(dr_type):(.+?)\s'
    #                              '(mergePlan):(.+?)(start time):(.+?)\s'
    #                              '(total use):(.+?)\s(count):(.+?)\s'
    #                              '(start day):(.+),\s(end day):(.+)')
    service_pattern = re.compile('(query):(.+?)\s(dr_type):(.+?)\s'
                                 '(mergePlan):(.+?)(start time):(.+?)\s'
                                 '(total use):\[(.*?)ms]\s(count):(.+?)\s'
                                 '(start day):(.+),\s(end day):(.+)')
    # service_pattern = re.compile('(start time):(.+?\s.+?)\s(?=|$)')
    # service_pattern = re.compile('(.+?):(.+?)\s')
    #logging.debug(service_pattern)
    result = service_pattern.findall(service_log)
    logging.debug(f'result is {result}')
    service_info_list = list(result[0])
    service_info_dic = {}
    for i, val in enumerate(service_info_list):
        #logging.debug(f'i is {i}, and value is {val}')
        if i % 2 == 0:
            key_value = {service_info_list[i]: service_info_list[i + 1]}
            logging.debug(f'key_value is {key_value}')
            service_info_dic.update(key_value)
    for key, value in service_info_dic.items():
        logging.info(f'key  is {key}, and value is {value}')
    logging.debug(f'query is {service_info_dic.get("query")}')

def excep_load_test(db_client, db, coll, log_path, file_regex, function_name):
    excep_pattern = re.compile(r'[A-Za-z.]*Exception')
    date_pattern = re.compile(r'(\d{4}-\d{1,2}-\d{1,2}\s\d{1,2}:\d{1,2}:\d{1,2}[,|.]\d+)')
    file_dic = stat_load.get_file_info(log_path, file_regex)
    now_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
    # logging.info(f'{file_dic}')
    for file_name in file_dic.keys():
        # logging.info(f'{file_name}')
        file_list = open(file_name, 'r', encoding='utf-8', errors='ignore')
        for exception_info in file_list:
            if not exception_info.__contains__('duplicate'):
                exception_info = exception_info.strip()
                excep_type = excep_pattern.findall(exception_info)
                time_info = date_pattern.findall(exception_info)
                time_info = time_info if len(time_info) != 0 else now_time
                now_time = time_info
                if len(excep_type) != 0:
                    # logging.info(f'{time_info}:{excep_type}')
                    # logging.info(exception_info)
                    excep_dic = update_dic(time_info[0], excep_type[0], exception_info, file_name, function_name)
                    mongo_writer.conn_insertone(db_client, db, coll, excep_dic)
                    logging.info(excep_dic)


# exception解析入库 解析异常类型和时间 包含异常信息的整条日志
# 时间如果不存在 使用上一次日志时间进行替换，使用now_time初始化为系统当前时间，保存上一条日志时间
def combine_excep_stat(db_client, db, coll, function_name, exception_info_list, file_name):
    excep_pattern = re.compile(r'[A-Za-z.]*Exception')
    date_pattern = re.compile(r'(\d{4}-\d{1,2}-\d{1,2}\s\d{1,2}:\d{1,2}:\d{1,2}[,|.]\d+)')
    # file_dic = stat_load.get_file_info(log_path, file_regex)
    now_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
    # logging.info(f'{file_dic}')
    # for file_name in file_dic.keys():
    # logging.info(f'{file_name}')
    # file_list = open(file_name, 'r', encoding='utf-8', errors='ignore')
    for exception_info in exception_info_list:
        if not exception_info.__contains__('duplicate'):
            exception_info = exception_info.strip()
            excep_type = excep_pattern.findall(exception_info)
            time_info = date_pattern.findall(exception_info)
            time_info = time_info if len(time_info) != 0 else now_time
            now_time = time_info
            if len(excep_type) != 0:
                # logging.info(f'{time_info}:{excep_type}')
                # logging.info(exception_info)
                excep_dic = update_dic(time_info[0], excep_type[0], exception_info, file_name, function_name)
                mongo_writer.conn_insertone(db_client, db, coll, excep_dic)
                logging.info(excep_dic)


def update_dic(time_info, excep_type, exception_info, file_name, function_name):
    dic = dict({"ExceptionType": excep_type,
                "ExceptionInfo": exception_info,
                "ExceptionTime": time_info,
                "FileName": file_name,
                "FunctionName": function_name})
    return dic


# # insert stat
# def insert_stat(mongo_client, stat_db_name, stat_coll_name, filename, file_modify_time_from_path):
#     start_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
#     status = 'start'
#     line_number = '-1'
#     condition = {"FileName": filename}
#     doc = {"$set": {'StartTime': start_time, 'Status': status, 'FileLineNumber': line_number,
#                     'FileName': filename, "FileModifyTime": file_modify_time_from_path}}
#     writer.conn_update(mongo_client, stat_db_name, stat_coll_name, condition, doc, True)
#
#
# # update stat
# def update_stat(mongo_client, stat_db_name, stat_coll_name, filename, line_number, file_modify_time_from_path):
#     end_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
#     status = 'finish'
#     condition = {"FileName": filename}
#     doc = {"$set": {'Status': status, 'FileLineNumber': line_number, 'EndTime': end_time,
#                     "FileModifyTime": file_modify_time_from_path}}
#     logging.info(f'conditon is: {condition}, doc is : {doc}')
#     writer.conn_update(mongo_client, stat_db_name, stat_coll_name, condition, doc, True)


if __name__ == '__main__':
    test()
    # # 数据库认证
    # username = 'root'
    # password = 'root'
    # mongos_host = '10.19.85.33'
    # mongos_port = 34000
    # db_name = 'test'
    # coll_name = 'stat_excep_20210129'
    # writer = mongo_writer
    # client = writer.auth(username, password, mongos_host, mongos_port)
    # stat_db = "test"
    # stat_coll = "excep_stat_load_stat_" + stat_load.get_date(0)
    # path = '/Users/mtr/PycharmProjects/mongoQuery/resource/exception'
    # regex = 'catalina'
    # func_name = 'emit'
    # # excep_load(client, db_name, coll_name, path, regex, func_name)
    #
    # file_info_dic_from_path = stat_load.get_file_info(path, regex)
    # deal_file_dic = stat_load.get_deal_file_dic(client, stat_db, stat_coll, file_info_dic_from_path)
    #
    # for name, num in deal_file_dic.items():
    #     file_modify_time = file_info_dic_from_path.get(name)
    #     insert_stat(client, stat_db, stat_coll, name, file_modify_time)
    #     exceptinfo_list = stat_load.read_stat_info(name, num)
    #     combine_excep_stat(client, db_name, coll_name, func_name, exceptinfo_list, name)
    #     update_stat(client, stat_db, stat_coll, name, num + len(exceptinfo_list), file_modify_time)
    # print('-----')
