import datetime
import logging
import re

from asiainfo.mongoapp.mongo import mongo_writer
from asiainfo.mongoapp.tool import stat_util, tool_util

# logging.basicConfig(level=logging.DEBUG,
#                     # filename='/Users/mtr/PycharmProjects/mongoQuery/resource/log/recv_stat_load.log',
#                     filemode='a',
#                     format='%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s')


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
    logging.debug(f'print list interval 2 {service_info_list[1::2]}')
    for i, val in enumerate(service_info_list):
        #logging.debug(f'i is {i}, and value is {val}')
        if i % 2 == 0:
            key_value = {service_info_list[i]: service_info_list[i + 1]}
            logging.debug(f'key_value is {key_value}')
            service_info_dic.update(key_value)
    for key, value in service_info_dic.items():
        logging.info(f'key  is {key}, and value is {value}')
    logging.debug(f'query is {service_info_dic.get("query")}')
    logging.info(service_info_dic)


# xdrservice日志解析入库 解析查询条件及耗时 包含整条日志
def combine_service_stat(db_client, db, coll, function_name, service_log_list_from_path, file_name):
    service_pattern = re.compile('(query):(.+?)\s(dr_type):(.+?)\s'
                                 '(mergePlan):(.+?)(start time):(.+?)\s'
                                 '(total use):\[(.*?)ms]\s(count):(.+?)\s'
                                 '(start day):(.+),\s(end day):(.+)')
    now_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
    for info in service_log_list_from_path:
        if info.__contains__('query:'):
            service_info = info.strip()
            logging.debug(f'service_info is {service_info}')
            service_info_list = service_pattern.findall(service_info)
            if service_info_list is None:
                continue
            logging.debug(f'service_info_list is {service_info_list}')
            service_info_list_replace = service_info_list[0][1::2]
            logging.debug(f'service_info_list_replace is {service_info_list_replace}')
            service_info_list = list(service_info_list_replace)
            # 判断service_info_list长度是否等于8 进行字段列表校验
            service_info_dic = update_dic(service_info_list, file_name, function_name)
            logging.debug(f'service_info_dic is {service_info_dic}')
            if service_info_dic is None:
                continue
            # 入库效率较低 考虑数据库性能问题 及单次插入问题，可修改为批量插入
            mongo_writer.conn_insertone(db_client, db, coll, service_info_dic)


def update_dic(service_info_list, file_name, function_name):
    dic = {}
    try:
        dic = dict({'Query': service_info_list[0],
                    'DrType': service_info_list[1],
                    'MergePlan': service_info_list[2],
                    'StartTime': service_info_list[3],
                    'TotalUse': int(service_info_list[4]),
                    'Count': int(service_info_list[5]),
                    'StartDay': service_info_list[6],
                    'EndDay': service_info_list[7],
                    'FileName': file_name,
                    'FunctionName': function_name})
    except Exception as e:
        logging.error(f'update_dic get Exception {e}')
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
    # test()
    # 数据库认证
    username = 'root'
    password = 'root'
    mongos_host = '10.19.85.33'
    mongos_port = 34000
    db_name = 'test'
    coll_name = 'stat_service'
    client = mongo_writer.auth(username, password, mongos_host, mongos_port)
    stat_db = "test"
    stat_coll = "service_stat_in"
    path = '/Users/mtr/PycharmProjects/mongoQuery/resource/xdr_service'
    regex = 'service'
    func_name = 'xdr_service'

    file_info_dic_from_path = tool_util.get_file_info(path, regex)
    deal_file_dic = tool_util.get_deal_file_dic(client, stat_db, stat_coll, file_info_dic_from_path)

    for name, num in deal_file_dic.items():
        file_modify_time = file_info_dic_from_path.get(name)
        stat_util.insert_stat(mongo_writer, client, stat_db, stat_coll, name, file_modify_time)
        service_log_list = tool_util.read_stat_info(name, num)
        combine_service_stat(client, db_name, coll_name, func_name, service_log_list, name)
        stat_util.update_stat(mongo_writer, client, stat_db, stat_coll, name, num + len(service_log_list), file_modify_time)
    print('-----')
