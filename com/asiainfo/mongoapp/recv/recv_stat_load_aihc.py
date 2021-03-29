import json as JSON

from asiainfo.mongoapp.mongo import mongo_writer
from asiainfo.mongoapp.tool import stat_util, tool_util

global roboter
global logger


# 初始化需要处理的所有文件 key = 文件名 value = -1 文件行  文件名来自filepath下
def init_file_deal_dic(file_info_from_path: dict):
    file_deal_dic = {}
    for key in file_info_from_path.keys():
        file_deal_dic.setdefault(key, -1)
    logger.debug(f'file_deal_dic is: {file_deal_dic}')
    return file_deal_dic


# 解析stat数据，组合为document
def combine_stat_doc(stat_line):
    stat_line_list = stat_line.split(';')
    combine_flag: bool = True
    tmp_list = []
    stat_dic = {"InputTime": tool_util.get_now_time(), "Ip": roboter.getNodeIp()}
    for line in stat_line_list:
        if not line.__contains__('{') and not line.__contains__('}') and combine_flag:
            logger.debug(f'line is {line}')
            update_dic(line, stat_dic)

        else:
            combine_flag = False
            tmp_list.append(line)
            tmp_list.append(';')
            if line.__contains__('}'):
                tmp_list.pop()
                tmp_line = ''.join(tmp_list)
                logger.debug(f'in if tmp is : {tmp_line}')
                update_dic(tmp_line, stat_dic)
                combine_flag = True
                tmp_list = []
    logger.debug(f'stat_dic is : {stat_dic}')
    return stat_dic


# 拆分stat信息中key value 组合dic 对需要累加的字段 入库时需要转为int
# string在aggregate语句中无法累加，转换类型需要在4.0支持
def update_dic(str_line, stat_dic_obj):
    key_str = str_line.split(':')[0]
    value_str = str_line[len(key_str) + 1:]
    if key_str.endswith("Lines"):
        value_str = int(value_str)
    logger.debug(f'key and value is {key_str}:{value_str}')
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
    password = 'Yjcsxdl_1218'
    mongos_host = '10.68.112.11'
    mongos_port = 22001
    db_name = 'aihc'
    coll_name = 'stat_recv'
    client = mongo_writer.auth(username, password, mongos_host, mongos_port)
    stat_db = "aihc_stat"
    stat_coll = "stat_recvstat_in"

    # 读文件获取stat文件信息入库
    path = '/data/mongo/docker_mnt/recv/stat'
    regex = 'recv'
    logger.info(f'recv_stat_load start, path is {path}, regex is {regex}')
    date_before = 1
    file_info_dic_from_path = tool_util.get_file_info(path, regex, date_before)
    file_dic = tool_util.get_deal_file_dic(client, stat_db, stat_coll, file_info_dic_from_path, roboter.getNodeIp())
    for name, num in file_dic.items():
        file_modify_time = file_info_dic_from_path.get(name)
        stat_util.insert_stat(mongo_writer, client, stat_db, stat_coll, name, file_modify_time, roboter.getNodeIp())
        statinfo_list = tool_util.read_stat_info(name, num)
        for stat_str in statinfo_list:
            stat_doc = combine_stat_doc(stat_str)
            mongo_writer.conn_insertone(client, db_name, coll_name, stat_doc)
        stat_util.update_stat(mongo_writer, client, stat_db, stat_coll,
                              name, num + len(statinfo_list), file_modify_time, roboter.getNodeIp())
    result = {"recv_stat_load": "succeed"}
    roboter.setContent(JSON.dumps(result))
    logger.info(f'recv_stat_load end')
