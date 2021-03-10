import re
import logging
import datetime
from asiainfo.mongoapp.mongo import mongo_writer
from asiainfo.mongoapp.tool import tool_util, stat_util

logging.basicConfig(level=logging.DEBUG,
                    # filename='/Users/mtr/PycharmProjects/mongoQuery/resource/log/exception_stat_load.log',
                    filemode='a',
                    format='%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s')


def test():
    # excep_log = '2021-02-02 09:52:55,385 INFO [XdrDecoder] ' \
    #             '[Executor task launch worker-51353]
    #             deal file:/file_dir/xdr_load/proc/load120_v65_3/
    #             online_ggprs_5g_SG_00008500_0001_20210202_117_61075718_94501_1_0.zxjf ' \
    #             'catch exception:java.lang.NullPointerException: xdr decode failed,no match for dr_type'
    excep_log = '2020-11-25 15:45:06.421 [DEBUG] [localhost-startStop-2] ' \
                '[org.springframework.core.env.PropertySourcesPropertyResolver:81] - ' \
                'trying original name [spring.profiles.default]. javax.naming.NameNotFoundException:'
    # excep_log = '2020-11-23 17:03:47.240 [DEBUG] [localhost-startStop-1] ' \
    #             '[org.springframework.jndi.JndiPropertySource:90] - JNDI lookup ' \
    #             'for name [spring.profiles.active] threw NamingException with message: ' \
    #             'Name [spring.profiles.active] is not bound in this Context. Unable to find ' \
    #             '[spring.profiles.active].. Returning null'
    # logging.info(f'{file_dic}')
    excep_pattern = re.compile(r'[A-Za-z.]*Exception')
    logging.debug(f'excep_pattern is {excep_pattern}')
    result = excep_pattern.findall(excep_log)
    logging.debug(f'result is {result}')
    date_pattern = re.compile(r'(\d{4}-\d{1,2}-\d{1,2}\s\d{1,2}:\d{1,2}:\d{1,2}[,|.]\d+)')
    date = date_pattern.findall(excep_log)
    logging.debug(f'date is {date}')


def excep_load_test(db_client, db, coll, log_path, file_regex, function_name):
    excep_pattern = re.compile(r'[A-Za-z.]*Exception')
    date_pattern = re.compile(r'(\d{4}-\d{1,2}-\d{1,2}\s\d{1,2}:\d{1,2}:\d{1,2}[,|.]\d+)')
    file_dic = tool_util.get_file_info(log_path, file_regex)
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
                    logging.debug(f'excep_dic is {excep_dic}')


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
                logging.debug(f'excep_dic is {excep_dic}')


def update_dic(time_info, excep_type, exception_info, file_name, function_name):
    dic = dict({"ExceptionType": excep_type,
                "ExceptionInfo": exception_info,
                "ExceptionTime": time_info,
                "FileName": file_name,
                "FunctionName": function_name})
    return dic


# regex path可配置 ，进行path和regex遍历  进行多种异常文件处理入库
if __name__ == '__main__':
    # 数据库认证
    username = 'root'
    password = 'root'
    mongos_host = '10.19.85.33'
    mongos_port = 34000
    db_name = 'test'
    coll_name = 'stat_excep'
    client = mongo_writer.auth(username, password, mongos_host, mongos_port)
    stat_db = "test"
    stat_coll = "stat_excep_in"
    path = '/Users/mtr/PycharmProjects/mongoQuery/resource/exception'
    regex = 'catalina'
    func_name = 'emit'
    # excep_load(client, db_name, coll_name, path, regex, func_name)

    logging.info(f'exception_stat_load start, path is {path}, regex is {regex}')
    file_info_dic_from_path = tool_util.get_file_info(path, regex)
    deal_file_dic = tool_util.get_deal_file_dic(client, stat_db, stat_coll, file_info_dic_from_path)

    for name, num in deal_file_dic.items():
        file_modify_time = file_info_dic_from_path.get(name)
        stat_util.insert_stat(mongo_writer, client, stat_db, stat_coll, name, file_modify_time)
        exceptinfo_list = tool_util.read_stat_info(name, num)
        combine_excep_stat(client, db_name, coll_name, func_name, exceptinfo_list, name)
        stat_util.update_stat(mongo_writer, client, stat_db, stat_coll, name,
                              num + len(exceptinfo_list), file_modify_time)
    logging.info('exception_stat_load end')
