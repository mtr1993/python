import logging
import os

from asiainfo.mongoapp.mongo import mongo_writer
from asiainfo.mongoapp.tool import stat_util, tool_util

logging.basicConfig(level=logging.DEBUG,
                    # filename='/Users/mtr/PycharmProjects/mongoQuery/resource/log/recv_stat_load.log',
                    filemode='a',
                    format='%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s')


# # 查询stat入库的任务stat信息,和filepath路径下文件比对，获取需要增量处理的stat文件名和行号
# def get_deal_file_dic(mongo_client, stat_db_name, stat_coll_name, file_info_dic):
#     condition = {'Status': 'finish'}
#     file_deal_dic = init_file_deal_dic(file_info_dic)
#     result_list = mongo_writer.conn_query(mongo_client, stat_db_name, stat_coll_name, condition)
#     logging.info(f'result_list is {result_list}')
#     if result_list is not None:
#         for doc in result_list:
#             file_name = doc.get('FileName')
#             file_modify_time_from_stat = doc.get("FileModifyTime")
#             file_line_number = doc.get("FileLineNumber")
#             file_modify_time_from_path = file_info_dic.get(file_name)
#             logging.debug(f'filename is {file_name}, file_modify_time_from_stat is {file_modify_time_from_stat} '
#                           f', file_modify_time_from_path  is {file_modify_time_from_path}'
#                           f'， file_line_number is {file_line_number}')
#             if file_modify_time_from_stat is not None and file_modify_time_from_stat == file_modify_time_from_path:
#                 file_deal_dic.pop(file_name)
#                 continue
#             else:
#                 file_deal_dic.update({file_name: file_line_number})
#     logging.info(f'file_deal_dic is {file_deal_dic}')
#     return file_deal_dic


# 初始化需要处理的所有文件 key = 文件名 value = -1 文件行  文件名来自filepath下
def init_file_deal_dic(file_info_from_path: dict):
    file_deal_dic = {}
    for key in file_info_from_path.keys():
        file_deal_dic.setdefault(key, -1)
    logging.debug(f'file_deal_dic is: {file_deal_dic}')
    return file_deal_dic


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


# # 读取stat文件中信息
# def read_stat_info(file_name, line_index):
#     file = open(file_name, 'r', encoding='utf-8', errors='ignore')
#     stat_list = []
#     for line_num, stat_line_str in enumerate(file):
#         if line_num <= line_index:
#             continue
#         stat_line_str = stat_line_str.strip('\n')
#         stat_list.append(stat_line_str)
#         #logging.info(f'line_num is: {line_num}, stat_line_str is: {stat_line_str}')
#     return stat_list


# # 读取stat文件中信息
# def read_stat_info(file_name, line_index):
#     stat_list = []
#     with open(file_name, 'r', encoding='utf-8', errors='ignore') as f_input:
#         for stat_line_str in islice(f_input, line_index + 1, None):
#             stat_line_str = stat_line_str.strip('\n')
#             stat_list.append(stat_line_str)
#             logging.info(f'stat_line_str is: {stat_line_str}')
#
#     return stat_list


# 解析stat数据，组合为document
def combine_stat_doc(stat_line):
    stat_line_list = stat_line.split(';')
    combine_flag: bool = True
    tmp_list = []
    stat_dic = {}
    for line in stat_line_list:
        if not line.__contains__('{') and not line.__contains__('}') and combine_flag:
            logging.debug(f'line is {line}')
            update_dic(line, stat_dic)

        else:
            combine_flag = False
            tmp_list.append(line)
            tmp_list.append(';')
            if line.__contains__('}'):
                tmp_list.pop()
                tmp_line = ''.join(tmp_list)
                logging.debug(f'in if tmp is : {tmp_line}')
                update_dic(tmp_line, stat_dic)
                combine_flag = True
                tmp_list = []
    logging.debug(f'stat_dic is : {stat_dic}')
    return stat_dic


# 拆分stat信息中key value 组合dic 对需要累加的字段 入库时需要转为int
# string在aggregate语句中无法累加，转换类型需要在4.0支持
def update_dic(str_line, stat_dic_obj):
    key_str = str_line.split(':')[0]
    value_str = str_line[len(key_str) + 1:]
    if key_str.endswith("Lines"):
        value_str = int(value_str)
    logging.debug(f'key and value is {key_str}:{value_str}')
    stat_dic_obj.update({key_str: value_str})


# xdrloadstat 查询，计算入库效率
if __name__ == '__main__':
    # stat_line = 'FileName:H_GYM-3_20200820151010.00679.sms.A;FileSize:19844;' \
    #             'Earlist:20200820;latest:20200820;ExtALines:0;TotalLines:17;' \
    #             'CorrectLines:17;ErrorLines:0;SendLines:17;NonTimeoutExceptionLines:0;' \
    #             'TimeoutExceptionLines:0;StartTime:20201216190420;EndTime:20201216190420;' \
    #             'Speed:61;TopicList:{CloudXdrTopic0.2:8;CloudXdrTopic1.2:4;CloudXdrTopic2.2:5;};' \
    #             'BillDateList:{20200820:17;};TargetLines:0'

    # 数据库认证
    username = 'root'
    password = 'root'
    mongos_host = '10.19.85.33'
    mongos_port = 34000
    db_name = 'test'
    coll_name = 'stat_recv'
    client = mongo_writer.auth(username, password, mongos_host, mongos_port)
    stat_db = "test"
    stat_coll = "recv_stat_in"

    # 读文件获取stat文件信息入库
    path = '/Users/mtr/PycharmProjects/mongoQuery/resource/recv'
    regex = 'recv'
    logging.info(f'recv_stat_load start, path is {path}, regex is {regex}')
    file_info_dic_from_path = get_file_info(path, regex)
    file_dic = tool_util.get_deal_file_dic(client, stat_db, stat_coll, file_info_dic_from_path)
    for name, num in file_dic.items():
        file_modify_time = file_info_dic_from_path.get(name)
        stat_util.insert_stat(mongo_writer, client, stat_db, stat_coll, name, file_modify_time)
        statinfo_list = tool_util.read_stat_info(name, num)
        for stat_str in statinfo_list:
            stat_doc = combine_stat_doc(stat_str)
            mongo_writer.conn_insertone(client, db_name, coll_name, stat_doc)
        stat_util.update_stat(mongo_writer, client, stat_db, stat_coll,
                              name, num + len(statinfo_list), file_modify_time)
    logging.info(f'recv_stat_load end')

    # 测试read_stat_info函数
    # name = '/Users/mtr/PycharmProjects/mongoQuery/resource/emit/xdrEmit_stat_emit33_4214_2020121619.txt'
    # num = -1
    # statinfo_list = read_stat_info(name, num)
    # print(len(statinfo_list))
    # read_stat_info('/Users/mtr/PycharmProjects/mongoQuery/resource/xdrEmit_stat_emit33_4214_2020121619.txt', -1)
    # get_date(1)
    # get_file_info('/Users/mtr/PycharmProjects/mongoQuery/resource/')

    # stat文件入库  task_stat信息记录
