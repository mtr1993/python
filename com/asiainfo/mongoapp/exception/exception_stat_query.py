from asiainfo.mongoapp.mongo import mongo_writer
import datetime
import json
import logging

logging.basicConfig(level=logging.DEBUG,
                    # filename='/Users/mtr/PycharmProjects/mongoQuery/resource/log/exception_stat_query.log',
                    filemode='a',
                    format='%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s')


def query_excep(client, db_name, coll_name, start_time, end_time, period):
    aggregate_sql = ('[{"$match":{"ExceptionTime":{"$gte":"starttime","$lte":"endtime"}}},'
                     '{"$group":{"_id":{"ExceptionType":"$ExceptionType","FunctionName":"$FunctionName",'
                     '"Ip":"$Ip"},"count":{"$sum":1}}}]')
    aggregate_sql = aggregate_sql.replace("starttime", str(start_time)).replace("endtime", str(end_time))
    logging.info(f'aggregate_sql is {aggregate_sql}')
    aggregate_sql_list = json.loads(aggregate_sql)
    result_list = mongo_writer.conn_aggregate(client, db_name, coll_name, aggregate_sql_list)
    logging.info(f'result_list is {result_list}')
    return result_list


# exception stat 查询，统计各类业务exception数量
if __name__ == '__main__':
    username = 'root'
    password = 'root'
    mongos_host = '10.19.85.33'
    mongos_port = 34000
    db_name = 'test'
    coll_name = 'stat_excep'
    period = 90
    client = mongo_writer.auth(username, password, mongos_host, mongos_port)
    end_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    start_time = (datetime.datetime.now() + datetime.timedelta(minutes=-period)).strftime('%Y-%m-%d %H:%M:%S')
    # start_time = datetime.datetime.strptime('2020-11-23 17:03:47', '%Y-%m-%d %H:%M:%S')
    # end_time = (start_time + datetime.timedelta(minutes=period)).strftime("%Y-%m-%d %H:%M:%S")
    # aggregate_sql = ('[{"$match":{"ExceptionTime":{"$gte":"starttime","$lte":"endtime"}}},'
    #                  '{"$group":{"_id":"$ExceptionType",'
    #                  '"ExceptionCount":{"$sum":1}}}]')
    # aggregate_sql = ('[{"$match":{"ExceptionTime":{"$gte":"starttime","$lte":"endtime"}}},'
    #                  '{"$group":{"_id":"$null",'
    #                  '"ExceptionCount":{"$sum":1}}}]')
    # aggregate_sql = ('[{"$match":{"ExceptionTime":{"$gte":"starttime","$lte":"endtime"}}},'
    #                  '{"$group":{"_id":{"ExceptionType":"$ExceptionType","FunctionName":"$FunctionName",'
    #                  '"Ip":"$Ip"},"count":{"$sum":1}}}]')
    # aggregate_sql = aggregate_sql.replace("starttime", str(start_time)).replace("endtime", str(end_time))
    # logging.info(f'aggregate_sql is {aggregate_sql}')
    # aggregate_sql_list = json.loads(aggregate_sql)
    result_list = query_excep(client, db_name, coll_name, start_time, end_time, period)
    speed = 0
    for doc in result_list:
        logging.debug(doc)
    if len(result_list) == 0:
        logging.info(f'result_list is empty')
