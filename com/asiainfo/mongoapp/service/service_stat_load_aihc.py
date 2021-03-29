import datetime
import re
import json as JSON

from asiainfo.mongoapp.mongo import mongo_writer
from asiainfo.mongoapp.tool import stat_util, tool_util
global roboter
global logger


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
            logger.debug(f'service_info is {service_info}')
            service_info_list = service_pattern.findall(service_info)
            if service_info_list is None:
                continue
            logger.debug(f'service_info_list is {service_info_list}')
            service_info_list_replace = service_info_list[0][1::2]
            logger.debug(f'service_info_list_replace is {service_info_list_replace}')
            service_info_list = list(service_info_list_replace)
            # 判断service_info_list长度是否等于8 进行字段列表校验
            service_info_dic = update_dic(service_info_list, file_name, function_name)
            logger.debug(f'service_info_dic is {service_info_dic}')
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
                    'FunctionName': function_name,
                    'InputTime':tool_util.get_now_time(),
                    "Ip": roboter.getNodeIp()})
    except Exception as e:
        logger.error(f'update_dic get Exception {e}')
    return dic


if __name__ == '__main__':
    # test()
    # 数据库认证
    username = 'root'
    password = 'Yjcsxdl_1218'
    mongos_host = '10.68.112.11'
    mongos_port = 22001
    db_name = 'aihc'
    coll_name = 'stat_service'
    client = mongo_writer.auth(username, password, mongos_host, mongos_port)
    stat_db = "aihc_stat"
    stat_coll = "stat_servicestat_in"
    path = '/data/mongo/docker_mnt/log_app/'
    regex = 'service'
    func_name = 'service'
    day_before = 1

    file_info_dic_from_path = tool_util.get_file_info(path, regex, day_before)
    deal_file_dic = tool_util.get_deal_file_dic(client, stat_db, stat_coll, file_info_dic_from_path, roboter.getNodeIp())

    for name, num in deal_file_dic.items():
        file_modify_time = file_info_dic_from_path.get(name)
        stat_util.insert_stat(mongo_writer, client, stat_db, stat_coll, name, file_modify_time, roboter.getNodeIp())
        service_log_list = tool_util.read_stat_info(name, num)
        combine_service_stat(client, db_name, coll_name, func_name, service_log_list, name)
        stat_util.update_stat(mongo_writer, client, stat_db, stat_coll, name, num + len(service_log_list), file_modify_time, roboter.getNodeIp())
    result = {"service_stat_load": "succeed"}
    roboter.setContent(JSON.dumps(result))
    logger.info(f'service stat load end')
