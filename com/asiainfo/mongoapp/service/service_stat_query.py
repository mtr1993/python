import datetime

from asiainfo.mongoapp.mongo import mongo_writer
import json

# service查询 stat，计算查询效率
if __name__ == '__main__':
    username = 'root'
    password = 'root'
    mongos_host = '10.19.85.33'
    mongos_port = 34000
    db_name = 'test'
    coll_name = 'stat_service_20210208'
    querystat = mongo_writer
    client = querystat.auth(username, password, mongos_host, mongos_port)
    print("-----")
    start_time = '20210202 09:24:48'
    print(start_time)
    period = 2
    end_time = (datetime.datetime.strptime(start_time, '%Y%m%d %H:%M:%S') +
                datetime.timedelta(minutes=period)).strftime("%Y%m%d %H:%M:%S")
    aggregate_sql = ('[{"$match":{"StartTime":{"$gte":"starttime","$lte":"endtime"}}},'
                     '{"$group":{"_id":null,'
                     '"QueryCount":{"$sum":1},'
                     '"QueryTimeCount":{"$sum":"$TotalUse"}}}]')
    aggregate_sql = aggregate_sql.replace("starttime", str(start_time)).replace("endtime", str(end_time))
    print(aggregate_sql)
    aggregate_sql_list = json.loads(aggregate_sql)
    result_list = querystat.conn_aggregate(client, db_name, coll_name, aggregate_sql_list)

    if len(result_list) == 0:
        print("result is empty")
        speed = 0
    else:
        speed = result_list[0].get("QueryCount") / result_list[0].get("QueryTimeCount") * 1000
    print(f"speed : {speed} 次/秒")
