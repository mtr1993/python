import datetime
import json as JSON
import re

from asiainfo.mongoapp.mongo import mongo_writer
from asiainfo.mongoapp.tool import tool_util, stat_util

global roboter
global logger


# exception解析入库 解析异常类型和时间 包含异常信息的整条日志
# 时间如果不存在 使用上一次日志时间进行替换，使用now_time初始化为系统当前时间，保存上一条日志时间
def combine_excep_stat(db_client, db, coll, function_name, exception_info_list, file_name):
    excep_pattern = re.compile(r'[A-Za-z.]*Exception')
    date_pattern = re.compile(r'(\d{4}-\d{1,2}-\d{1,2}\s\d{1,2}:\d{1,2}:\d{1,2}[,|.]\d+)')
    # file_dic = stat_load.get_file_info(log_path, file_regex)
    now_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
    # logger.info(f'{file_dic}')
    # for file_name in file_dic.keys():
    # logger.info(f'{file_name}')
    # file_list = open(file_name, 'r', encoding='utf-8', errors='ignore')
    for exception_info in exception_info_list:
        if not exception_info.__contains__('duplicate'):
            exception_info = exception_info.strip()
            excep_type = excep_pattern.findall(exception_info)
            time_info = date_pattern.findall(exception_info)
            time_info = time_info if len(time_info) != 0 else now_time
            now_time = time_info
            if len(excep_type) != 0:
                # logger.info(f'{time_info}:{excep_type}')
                # logger.info(exception_info)
                excep_dic = update_dic(time_info[0], excep_type[0], exception_info, file_name, function_name)
                mongo_writer.conn_insertone(db_client, db, coll, excep_dic)
                logger.debug(f'excep_dic is {excep_dic}')


def update_dic(time_info, excep_type, exception_info, file_name, function_name):
    dic = dict({"ExceptionType": excep_type,
                "ExceptionInfo": exception_info,
                "ExceptionTime": time_info,
                "FileName": file_name,
                "FunctionName": function_name,
                'InputTime': tool_util.get_now_time(),
                'Ip': roboter.getNodeIp()})
    return dic


# regex path可配置 ，进行path和regex遍历  进行多种异常文件处理入库
if __name__ == '__main__':
    # 数据库认证
    username = 'root'
    password = 'Yjcsxdl_1218'
    mongos_host = '10.68.112.11'
    mongos_port = 22001
    db_name = 'aihc'
    coll_name = 'stat_excep'
    client = mongo_writer.auth(username, password, mongos_host, mongos_port)
    stat_db = "aihc_stat"
    stat_coll = "stat_excepstat_in"
    path = '/data/mongo/docker_mnt/log_app/'
    regex = 'service'
    func_name = 'service'
    before_day = 1
    # excep_load(client, db_name, coll_name, path, regex, func_name)

    logger.info(f'exception_stat_load start, path is {path}, regex is {regex}')
    file_info_dic_from_path = tool_util.get_file_info(path, regex, before_day)
    deal_file_dic = tool_util.get_deal_file_dic(client, stat_db, stat_coll, file_info_dic_from_path)

    for name, num in deal_file_dic.items():
        file_modify_time = file_info_dic_from_path.get(name)
        logger.info
        stat_util.insert_stat(mongo_writer, client, stat_db, stat_coll, name, file_modify_time)
        exceptinfo_list = tool_util.read_stat_info(name, num)
        combine_excep_stat(client, db_name, coll_name, func_name, exceptinfo_list, name)
        stat_util.update_stat(mongo_writer, client, stat_db, stat_coll, name,
                              num + len(exceptinfo_list), file_modify_time)
    result = {"service_exception_in": "succeed"}
    roboter.setContent(JSON.dumps(result))
    logger.info('service_exception_in end')
