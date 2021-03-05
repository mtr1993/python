from asiainfo.mongoapp.mongo import mongo_writer
import json

# emit stat 查询，计算emit采集效率
if __name__ == '__main__':
    username = 'root'
    password = 'root'
    mongos_host = '10.19.85.33'
    mongos_port = 34000
    db_name = 'test'
    coll_name = 'stat_emit_20210129'
    querystat = mongo_writer
    client = querystat.auth(username, password, mongos_host, mongos_port)
    print("-----")
    start_time = 20201216000000
    period = 235959
    end_time = start_time + period
    aggregate_sql = ('[{"$match":{"StartTime":{"$gte":"starttime","$lte":"endtime"}}},'
                     '{"$group":{"_id":null,'
                     '"CorrectLines":{"$sum":"$CorrectLines"},'
                     '"ErrorLines":{"$sum": "$ErrorLines"},'
                     '"SendLines":{"$sum":"$SendLines"},'
                     '"NonTimeoutExceptionLines":{"$sum":"$NonTimeoutExceptionLines"},'
                     '"TimeoutExceptionLines":{"$sum":"$TimeoutExceptionLines"}}}]')
    aggregate_sql = aggregate_sql.replace("starttime", str(start_time)).replace("endtime", str(end_time))
    print(aggregate_sql)
    aggregate_sql_list = json.loads(aggregate_sql)
    result = querystat.conn_aggregate(client, db_name, coll_name, aggregate_sql_list)
    print(result[0])
    speed = result[0].get("SendLines") / period
    print(f"speed : {speed} 条/秒")
