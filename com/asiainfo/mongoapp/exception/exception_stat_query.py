from asiainfo.mongoapp.mongo import mongo_writer
import datetime
import json
import logging

logging.basicConfig(level=logging.DEBUG,
                    # filename='/Users/mtr/PycharmProjects/mongoQuery/resource/log/exception_stat_query.log',
                    filemode='a',
                    format='%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s')


# exception stat 查询，统计各类业务exception数量
if __name__ == '__main__':
    username = 'root'
    password = 'root'
    mongos_host = '10.19.85.33'
    mongos_port = 34000
    db_name = 'test'
    coll_name = 'stat_excep'
    querystat = mongo_writer
    client = querystat.auth(username, password, mongos_host, mongos_port)
    start_time = datetime.datetime.strptime('2020-11-23 17:03:47', '%Y-%m-%d %H:%M:%S')
    period = 90
    end_time = (start_time + datetime.timedelta(days=period)).strftime("%Y-%m-%d %H:%M:%S")
    # aggregate_sql = ('[{"$match":{"ExceptionTime":{"$gte":"starttime","$lte":"endtime"}}},'
    #                  '{"$group":{"_id":"$ExceptionType",'
    #                  '"ExceptionCount":{"$sum":1}}}]')
    aggregate_sql = ('[{"$match":{"ExceptionTime":{"$gte":"starttime","$lte":"endtime"}}},'
                     '{"$group":{"_id":"$null",'
                     '"ExceptionCount":{"$sum":1}}}]')
    aggregate_sql = aggregate_sql.replace("starttime", str(start_time)).replace("endtime", str(end_time))
    logging.info(f'aggregate_sql is {aggregate_sql}')
    aggregate_sql_list = json.loads(aggregate_sql)
    result_list = querystat.conn_aggregate(client, db_name, coll_name, aggregate_sql_list)
    speed = 0
    for doc in result_list:
        logging.debug(doc)
    if len(result_list) == 0:
        logging.info(f'result_list is empty')
    else:
        speed = result_list[0].get("ExceptionCount") / period
    logging.info(f"speed : {speed} 次/天")
