import datetime
import json
import logging

from asiainfo.mongoapp.mongo import mongo_writer

# logging.basicConfig(level=logging.DEBUG,
#                     # filename='/Users/mtr/PycharmProjects/mongoQuery/resource/log/emit_stat_query.log',
#                     filemode='a',
#                     format='%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s')


def query(client, db_name, coll_name,  start_time, end_time, period):
    # username = 'root'
    # password = 'root'
    # mongos_host = '10.19.85.33'
    # mongos_port = 34000
    # db_name = 'test'
    # coll_name = 'stat_emit'
    # client = mongo_writer.auth(username, password, mongos_host, mongos_port)
    # 使用系统当前时间前时间范围进行数据查找
    # end_time = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    # # period = -5
    # start_time = (datetime.datetime.now()+datetime.timedelta(minutes=period)).strftime('%Y%m%d%H%M%S')
    aggregate_sql = ('[{"$match":{"StartTime":{"$gte":"starttime","$lte":"endtime"}}},'
                     '{"$group":{"_id":null,'
                     '"CorrectLines":{"$sum":"$CorrectLines"},'
                     '"ErrorLines":{"$sum": "$ErrorLines"},'
                     '"SendLines":{"$sum":"$SendLines"},'
                     '"NonTimeoutExceptionLines":{"$sum":"$NonTimeoutExceptionLines"},'
                     '"TimeoutExceptionLines":{"$sum":"$TimeoutExceptionLines"}}}]')
    aggregate_sql = aggregate_sql.replace("starttime", str(start_time)).replace("endtime", str(end_time))
    logging.info(f'aggregate_sql is {aggregate_sql}')
    aggregate_sql_list = json.loads(aggregate_sql)
    result_list = mongo_writer.conn_aggregate(client, db_name, coll_name, aggregate_sql_list)
    logging.info(f'result_list is {result_list}')
    if len(result_list) == 0:
        speed = 0
    else:
        speed = result_list[0].get("SendLines") / period
    logging.info(f"speed : {speed} 条/秒")
    return speed


# emit stat 查询，计算emit采集效率
if __name__ == '__main__':
    username = 'root'
    password = 'root'
    mongos_host = '10.19.85.33'
    mongos_port = 34000
    db_name = 'test'
    coll_name = 'stat_emit'
    time_period = -5
    end_time = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    start_time = (datetime.datetime.now() + datetime.timedelta(minutes=time_period)).strftime('%Y%m%d%H%M%S')
    client = mongo_writer.auth(username, password, mongos_host, mongos_port)
    speed = query(client, db_name, coll_name, start_time, end_time, time_period)
    logging.info(f"speed : {speed} 条/秒")
