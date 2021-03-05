from asiainfo.mongoapp.mongo import mongo_writer
import json


# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press ⌘F8 to toggle the breakpoint.
    info = "failed"
    info2 = "test"
    # logging.error(f'logging test {info}, the result is {info2}')


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('PyCharm')
    username = 'root'
    password = 'root'
    mongos_host = '10.19.85.33'
    mongos_port = 34000
    db_name = 'test'
    coll_name = 'test_20210125'
    querystat = mongo_writer
    client = querystat.auth(username, password, mongos_host, mongos_port)
    print("-----")

    # # 新增数据
    # insert_document = {"_id": 99, "name": "insertTest99", "info": "python_insert_mongo99"}
    # querystat.conn_insertone(client, db_name, coll_name, insert_document)
    # # 查询数据
    # query_condition = {"_id": 100}
    # querystat.conn_query(client, db_name, coll_name, query_condition)
    #
    # # 新增数据 list
    # insert_document_list = [{"_id": 100, "name": "insertTest100", "info": "python_insert_mongo100"},
    #                    {"_id": 101, "name": "insertTest101", "info": "python_insert_mongo101"}]
    # querystat.conn_insertmany(client, db_name, coll_name, insert_document_list)
    # # 查询数据
    # query_condition = {"_id": {"$ne": 1}}
    # querystat.conn_query(client, db_name, coll_name, query_condition)
    #
    # # 更新数据
    # new_values = {"$set": {"info": "python_update_mongo100"}}
    # querystat.conn_update(client, db_name, coll_name, query_condition, new_values)
    # # 查询数据
    # query_condition = {"_id": 100}
    # querystat.conn_query(client, db_name, coll_name, query_condition)
    #
    # # 删除数据
    # delete_condition = query_condition
    # querystat.conn_delete(client, db_name, coll_name, delete_condition)
    # # 查询数据
    # query_condition = {"_id": 100}
    # querystat.conn_query(client, db_name, coll_name, query_condition)
    #
    # # aggregate_sql = ('{$group:{_id:"$FileName",'
    # #                        '"ReadXdrCount":{$sum:"$ReadXdrCount"},'
    # #                        '"DeleteOkCount":{$sum: "$DeleteOkCount"},'
    # #                        '"SkippedXdrCount":{$sum:"$SkippedXdrCount"},'
    # #                        '"WriteOkXdrCount":{$sum:"$WriteOkXdrCount"},'
    # #                        '"WriteFailXdrCount":{$sum:"$WriteFailXdrCount"},'
    # #                        '"WriteFailMongoServerErrXdrCount":{$sum: "$WriteFailMongoServerErrXdrCount"},'
    # #                        '"WriteFailMongoErrXdrCount":{$sum:"$WriteFailMongoErrXdrCount"},'
    # #                        '"WriteFailSocketErrXdrCount":{$sum:"$WriteFailSocketErrXdrCount"},'
    # #                        '"WriteFailBaseErrXdrCount":{$sum:"$WriteFailBaseErrXdrCount"},'
    # #                        '"WriteDuplicateXdrCount":{$sum: "$WriteDuplicateXdrCount"},'
    # #                        '"WriteNotRecycleCount":{$sum:"$WriteNotRecycleCount"}}}')
    #
    start_time = "20210107000000"
    end_time = "20210107235959"
    aggregate_sql = ('[{"$match":{"StartTime":{"$gte":"start_time","$lte":"end_time"}}},'
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
    aggregate_sql.replace("start_time", start_time).replace("end_time", end_time)
    print(aggregate_sql)
    aggregate_sql_list = json.loads(aggregate_sql)
    result = querystat.conn_aggregate(client, "stat_redo_65", "stat_xdr_in_20210107", aggregate_sql_list)
    print(result)