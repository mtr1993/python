import datetime
import logging

from asiainfo.mongoapp.mongo import mongo_writer
import json

# logging.basicConfig(level=logging.DEBUG,
#                     # filename='/Users/mtr/PycharmProjects/mongoQuery/resource/log/load_stat_query.log',
#                     filemode='a',
#                     format='%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s')
from asiainfo.mongoapp.tool import tool_util


def query(client, db_name, coll_name, start_time, end_time, period):
    # start_time = 20210107000000
    # period = 235959
    # end_time = start_time + period
    aggregate_sql = ('[{"$match":{"StartTime":{"$gte":"starttime","$lte":"endtime"}}},'
                     '{"$group":{"_id":null,'
                     '"ReadXdrCount":{"$sum":"$ReadXdrCount"},'
                     '"DeleteOkCount":{"$sum": "$DeleteOkCount"},'
                     '"SkippedXdrCount":{"$sum":"$SkippedXdrCount"},'
                     '"WriteOkXdrCount":{"$sum":"$WriteOkXdrCount"},'
                     '"WriteFailXdrCount":{"$sum":"$WriteFailXdrCount"},'
                     '"WriteFailMongoServerErrXdrCount":{"$sum": "$WriteFailMongoServerErrXdrCount"},'
                     '"WriteFailMongoErrXdrCount":{"$sum":"$WriteFailMongoErrXdrCount"},'
                     '"WriteFailSocketErrXdrCount":{"$sum":"$WriteFailSocketErrXdrCount"},'
                     '"WriteFailBaseErrXdrCount":{"$sum":"$WriteFailBaseErrXdrCount"},'
                     '"WriteDuplicateXdrCount":{"$sum": "$WriteDuplicateXdrCount"},'
                     '"WriteNotRecycleCount":{"$sum":"$WriteNotRecycleCount"}}}]')
    aggregate_sql = aggregate_sql.replace("starttime", str(start_time)).replace("endtime", str(end_time))
    logging.info(f'aggregate_sql is {aggregate_sql}')
    aggregate_sql_list = json.loads(aggregate_sql)
    result_list = mongo_writer.conn_aggregate(client, db_name, coll_name, aggregate_sql_list)
    logging.info(f'result_list is {result_list}')
    if len(result_list) == 0:
        speed = 0
    else:
        speed = result_list[0].get("ReadXdrCount") / period
    logging.info(f"speed : {speed} 条/秒")
    return speed


# xdrload stat 查询，计算入库效率
if __name__ == '__main__':
    username = 'root'
    password = 'root'
    mongos_host = '10.19.85.33'
    mongos_port = 34000
    db_name = 'stat_redo_65'
    coll_name = 'stat_xdr_in_'+tool_util.get_date(0)
    time_period = -5
    end_time = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    start_time = (datetime.datetime.now() + datetime.timedelta(minutes=time_period)).strftime('%Y%m%d%H%M%S')
    client = mongo_writer.auth(username, password, mongos_host, mongos_port)
    speed = query(client, db_name, coll_name, start_time, end_time, time_period)
    logging.info(f"speed : {speed} 条/秒")
