import json as JSON
import os
import re

from asiainfo.mongoapp.mongo import mongo_writer
from asiainfo.mongoapp.tool import tool_util, stat_util

global roboter
global logger


# 获取 path_of_mongo_data 下含目录名为rollback_dir的bson文件 即rollback文件
def get_rollback_file_info(path_of_mongo_data, rollback_dir, pattern):
    rollback_files_dic = {}
    for root, dirs, files in os.walk(path_of_mongo_data):
        if not str(root).__contains__(rollback_dir):
            continue
        logger.debug(f'files is: {files}')
        for file in files:
            full_path_of_file = os.path.join(root, file)
            if not pattern.match(full_path_of_file):
                continue
            file_modify_time_from_path = os.path.getmtime(full_path_of_file)
            logger.debug(f'root is {root}, file is {file} , full_path_of_file is : {full_path_of_file}')
            rollback_files_dic.update({full_path_of_file: file_modify_time_from_path})
    return rollback_files_dic


# 使用robot-node获取Ip
def combine_rollback_stat(db_client, db, coll, function_name, file_name):
    stat_dic = dict({"FileName": file_name,
                     "InputTime": tool_util.get_now_time(),
                     "FunctionName": function_name,
                     "Ip": roboter.getNodeIp()})
    mongo_writer.conn_insertone(db_client, db, coll, stat_dic)


if __name__ == '__main__':
    # 数据库认证
    username = 'root'
    password = 'Yjcsxdl_1218'
    mongos_host = '10.68.112.11'
    mongos_port = 22001
    db_name = 'aihc'
    coll_name = 'stat_rollback'
    client = mongo_writer.auth(username, password, mongos_host, mongos_port)
    stat_db = "aihc_stat"
    stat_coll = "stat_rollbackstat_in"
    mongo_data_dir_name = '/db1/mongo/mongo_data1/'
    rollback_dir_name = 'rollback'
    bson_pattern = re.compile(r'.*\.bson$')
    func_name = 'rollback'
    file_info_dic_from_path = get_rollback_file_info(mongo_data_dir_name, rollback_dir_name, bson_pattern)
    file_dic = tool_util.get_deal_file_dic(client, stat_db, stat_coll, file_info_dic_from_path,roboter.getNodeIp())
    logger.info(f'files_dic is {file_dic}')
    for name in file_dic.keys():
        file_modify_time = file_info_dic_from_path.get(name)
        stat_util.insert_stat(mongo_writer, client, stat_db, stat_coll, name, file_modify_time, roboter.getNodeIp())
        combine_rollback_stat(client, db_name, coll_name, func_name, name)
        stat_util.update_stat(mongo_writer, client, stat_db, stat_coll, name, 1, file_modify_time, roboter.getNodeIp())
    result = {"rollback_stat_load": "succeed"}
    roboter.setContent(JSON.dumps(result))
    logger.info(f'rollback_stat_load end')
