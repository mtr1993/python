import re
import stat_load
import logging
import datetime
import mongo_writer


def test():
    # excep_log = '2021-02-02 09:52:55,385 INFO [XdrDecoder] ' \
    #             '[Executor task launch worker-51353] deal file:/file_dir/xdr_load/proc/load120_v65_3/online_ggprs_5g_SG_00008500_0001_20210202_117_61075718_94501_1_0.zxjf ' \
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
    print(excep_pattern)
    result = excep_pattern.findall(excep_log)
    print(result)
    date_pattern = re.compile(r'(\d{4}-\d{1,2}-\d{1,2}\s\d{1,2}:\d{1,2}:\d{1,2}[,|.]\d+)')
    date = date_pattern.findall(excep_log)
    print(date)


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
                excep_dic = {}
                exception_info = exception_info.strip()
                excep_type = excep_pattern.findall(exception_info)
                time_info = date_pattern.findall(exception_info)
                time_info = time_info if len(time_info) != 0 else now_time
                now_time = time_info
                if len(excep_type) != 0:
                    # logging.info(f'{time_info}:{excep_type}')
                    # logging.info(exception_info)
                    update_dic(time_info[0], excep_type[0], exception_info, file_name, excep_dic, function_name)
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
            excep_dic = {}
            exception_info = exception_info.strip()
            excep_type = excep_pattern.findall(exception_info)
            time_info = date_pattern.findall(exception_info)
            time_info = time_info if len(time_info) != 0 else now_time
            now_time = time_info
            if len(excep_type) != 0:
                # logging.info(f'{time_info}:{excep_type}')
                # logging.info(exception_info)
                update_dic(time_info[0], excep_type[0], exception_info, file_name, excep_dic, function_name)
                mongo_writer.conn_insertone(db_client, db, coll, excep_dic)
                logging.info(excep_dic)


def update_dic(time_info, excep_type, exception_info, file_name, excep_dic, function_name):
    excep_dic.update({"ExceptionType": excep_type})
    excep_dic.update({"ExceptionInfo": exception_info})
    excep_dic.update({"ExceptionTime": time_info})
    excep_dic.update({"FileName": file_name})
    excep_dic.update({"FunctionName": function_name})


# insert stat
def insert_stat(mongo_client, stat_db_name, stat_coll_name, filename, file_modify_time):
    start_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
    status = 'start'
    line_number = '-1'
    condition = {"FileName": filename}
    doc = {"$set": {'StartTime': start_time, 'Status': status, 'FileLineNumber': line_number,
                    'FileName': filename, "FileModifyTime": file_modify_time}}
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


if __name__ == '__main__':
    # 数据库认证
    username = 'root'
    password = 'root'
    mongos_host = '10.19.85.33'
    mongos_port = 34000
    db_name = 'test'
    coll_name = 'stat_excep_20210129'
    writer = mongo_writer
    client = writer.auth(username, password, mongos_host, mongos_port)
    stat_db = "test"
    stat_coll = "excep_stat_load_stat_" + stat_load.get_date(0)
    path = '/Users/mtr/PycharmProjects/mongoQuery/resource/exception'
    regex = 'catalina'
    func_name = 'emit'
    # excep_load(client, db_name, coll_name, path, regex, func_name)

    file_info_dic_from_path = stat_load.get_file_info(path, regex)
    file_dic = stat_load.get_deal_file_dic(client, stat_db, stat_coll, file_info_dic_from_path)

    for name, num in file_dic.items():
        file_modify_time = file_info_dic_from_path.get(name)
        insert_stat(client, stat_db, stat_coll, name, file_modify_time)
        exceptinfo_list = stat_load.read_stat_info(name, num)
        combine_excep_stat(client, db_name, coll_name, func_name, exceptinfo_list, name)
        update_stat(client, stat_db, stat_coll, name, num + len(exceptinfo_list), file_modify_time)
    print('-----')