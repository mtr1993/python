import mongo_writer
import datetime
import json
import logging

# exception stat 查询，统计各类业务exception数量
if __name__ == '__main__':
    username = 'root'
    password = 'root'
    mongos_host = '10.19.85.33'
    mongos_port = 34000
    db_name = 'test'
    coll_name = 'stat_rollback_20210129'
    querystat = mongo_writer
    client = querystat.auth(username, password, mongos_host, mongos_port)
    print("-----")
    start_time = datetime.datetime.strptime('2021-02-05 09:03:47', '%Y-%m-%d %H:%M:%S')
    period = 1
    end_time = (start_time + datetime.timedelta(days=period)).strftime("%Y-%m-%d %H:%M:%S")
    aggregate_sql = ('[{"$match":{"InputTime":{"$gte":"starttime","$lte":"endtime"}}},'
                     '{"$group":{"_id":null,'
                     '"rollbackfilecount":{"$sum":1}}}]')
    aggregate_sql = aggregate_sql.replace("starttime", str(start_time)).replace("endtime", str(end_time))
    logging.info(aggregate_sql)
    aggregate_sql_list = json.loads(aggregate_sql)
    result = querystat.conn_aggregate(client, db_name, coll_name, aggregate_sql_list)
    for doc in result:
        logging.debug(doc)
        rollbackfilecount = doc.get('rollbackfilecount')
    logging.info(f"rollbackfilecount : {rollbackfilecount} 个文件")
