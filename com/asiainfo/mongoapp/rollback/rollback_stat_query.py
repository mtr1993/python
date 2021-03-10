from asiainfo.mongoapp.mongo import mongo_writer
import datetime
import json
import logging
logging.basicConfig(level=logging.DEBUG,
                    # filename='/Users/mtr/PycharmProjects/mongoQuery/resource/log/recv_stat_load.log',
                    filemode='a',
                    format='%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s')


# rollback stat 查询，统计各台主机rollback文件数量
# 统计InputTime为当天时间的rollback记录
if __name__ == '__main__':
    username = 'root'
    password = 'root'
    mongos_host = '10.19.85.33'
    mongos_port = 34000
    db_name = 'test'
    coll_name = 'stat_rollback'
    client = mongo_writer.auth(username, password, mongos_host, mongos_port)
    start_time = datetime.datetime.strptime('2021-03-08 00:00:00', '%Y-%m-%d %H:%M:%S')
    period = 1
    end_time = (start_time + datetime.timedelta(days=period)).strftime("%Y-%m-%d %H:%M:%S")
    aggregate_sql = ('[{"$match":{"InputTime":{"$gte":"starttime","$lte":"endtime"}}},'
                     '{"$group":{"_id":null,'
                     '"rollbackfilecount":{"$sum":1}}}]').replace("starttime", str(start_time)).\
        replace("endtime", str(end_time))
    logging.info(f'aggregate_sql is {aggregate_sql}')
    aggregate_sql_list = json.loads(aggregate_sql)
    result_list = mongo_writer.conn_aggregate(client, db_name, coll_name, aggregate_sql_list)
    logging.info(f'result_list is {result_list}')
    if len(result_list) == 0:
        rollbackfilecount = 0
    else:
        rollbackfilecount = result_list[0].get('rollbackfilecount')
    logging.info(f"rollbackfilecount : {rollbackfilecount} 个文件")
