import mongo_writer
import json

# xdrload stat 查询，计算入库效率
if __name__ == '__main__':
    username = 'root'
    password = 'root'
    mongos_host = '10.19.85.33'
    mongos_port = 34000
    db_name = 'test'
    coll_name = 'test_20210125'
    querystat = mongo_writer
    client = querystat.auth(username, password, mongos_host, mongos_port)
    print("-----")
    start_time = 20210107000000
    period = 235959
    end_time = start_time + period
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
    print(aggregate_sql)
    aggregate_sql_list = json.loads(aggregate_sql)
    result = querystat.conn_aggregate(client, "stat_redo_65", "stat_xdr_in_20210107", aggregate_sql_list)
    print(result[0])
    speed = result[0].get("ReadXdrCount") / period
    print(f"speed : {speed} 条/秒")
