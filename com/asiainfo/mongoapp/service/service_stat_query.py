import datetime
import logging

from asiainfo.mongoapp.mongo import mongo_writer
import json

#
# logging.basicConfig(level=logging.DEBUG,
#                     # filename='/Users/mtr/PycharmProjects/mongoQuery/resource/log/recv_stat_load.log',
#                     filemode='a',
#                     format='%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s')


def query(client, db_name, coll_name, start_time, end_time, period):
    start_time = datetime.datetime.strptime(start_time, '%Y%m%d%H%M%S').strftime('%Y%m%d %H:%M:%S')
    end_time = datetime.datetime.strptime(end_time, '%Y%m%d%H%M%S').strftime('%Y%m%d %H:%M:%S')
    aggregate_sql = ('[{"$match":{"StartTime":{"$gte":"starttime","$lte":"endtime"}}},'
                     '{"$group":{"_id":null,'
                     '"QueryCount":{"$sum":1},'
                     '"QueryTimeCount":{"$sum":"$TotalUse"}}}]')
    aggregate_sql = aggregate_sql.replace("starttime", str(start_time)).replace("endtime", str(end_time))
    logging.info(f'aggregate_sql is {aggregate_sql}')
    aggregate_sql_list = json.loads(aggregate_sql)
    result_list = mongo_writer.conn_aggregate(client, db_name, coll_name, aggregate_sql_list)
    logging.info(f'result_list is {result_list}')
    if len(result_list) == 0:
        logging.info("result is empty")
        speed = 0
    else:
        try:
            speed = result_list[0].get("QueryCount") / period
        except Exception as e:
            logging.error(e)
            speed = 0
    logging.info(f"speed : {speed} 次/分钟")
    return speed


# service查询 stat，计算查询效率
if __name__ == '__main__':
    username = 'root'
    password = 'root'
    mongos_host = '10.19.85.33'
    mongos_port = 34000
    db_name = 'test'
    coll_name = 'stat_service_20210208'
    start_time = "20210202092448"
    end_time = "20210203092448"
    time_period = -5
    client = mongo_writer.auth(username, password, mongos_host, mongos_port)
    speed = query(client, db_name, coll_name, start_time, end_time, time_period)
    logging.info(f"speed : {speed} 秒/次")
