# insert stat
import logging
import datetime
# logging.basicConfig(level=logging.INFO,
#                     filename='/Users/mtr/PycharmProjects/mongoQuery/resource/log/stat_util.log',
#                     filemode='a',
#                     format='%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s')


def insert_stat(mongo_writer, mongo_client, stat_db_name, stat_coll_name, filename, file_modify_time_from_path):
    start_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
    status = 'start'
    line_number = '-1'
    condition = {"FileName": filename}
    doc = {"$set": {'StartTime': start_time, 'Status': status, 'FileLineNumber': line_number,
                    'FileName': filename, "FileModifyTime": file_modify_time_from_path}}
    mongo_writer.conn_update(mongo_client, stat_db_name, stat_coll_name, condition, doc, True)


# update stat
def update_stat(mongo_writer, mongo_client, stat_db_name, stat_coll_name, filename, line_number, file_modify_time_from_path):
    end_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
    status = 'finish'
    condition = {"FileName": filename}
    doc = {"$set": {'Status': status, 'FileLineNumber': line_number, 'EndTime': end_time,
                    "FileModifyTime": file_modify_time_from_path}}
    logging.info(f'conditon is: {condition}, doc is : {doc}')
    mongo_writer.conn_update(mongo_client, stat_db_name, stat_coll_name, condition, doc, True)
