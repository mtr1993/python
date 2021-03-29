# emit recv load service
import datetime
import json
import logging

from asiainfo.mongoapp.tool import tool_util

global roboter
global logger

from asiainfo.mongoapp.emit import emit_stat_query
from asiainfo.mongoapp.load import load_stat_query
from asiainfo.mongoapp.mongo import mongo_writer
from asiainfo.mongoapp.recv import recv_stat_query
from asiainfo.mongoapp.service import service_stat_query


# logging.basicConfig(level=logging.DEBUG,
#                     filename='/home/aihc/bhc/robot-node/.toolkit/logs/stat_query.log',
#                     filemode='w',
#                     format='%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s')


def mongo_client_auth(username, password, mongos_host, mongos_port):
    client = mongo_writer.auth(username, password, mongos_host, mongos_port)
    return client


if __name__ == '__main__':
    # logging.info('start stat_query')
    db_name = 'aihc'
    result = {}
    index = -1
    period = 100
    end_time = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    start_time = (datetime.datetime.now() + datetime.timedelta(minutes=-period)).strftime('%Y%m%d%H%M%S')
    mongo_client = mongo_client_auth('root', 'Yjcsxdl_1218', '10.68.112.11', 22001)
    # TODO query emit speed
    emit_coll_name = 'stat_emit'
    # emit_speed = emit_stat_query.query(mongo_client, db_name, emit_coll_name, start_time, end_time, period)
    result['sum' + str(index + 1)] = 100

    # TODO query recv speed  个/分钟
    recv_coll_name = 'stat_recv'
    recv_speed = recv_stat_query.query(mongo_client, db_name, recv_coll_name, start_time, end_time, period)
    result['sum' + str(index + 2)] = round(recv_speed, 2)

    # TODO query load speed  条/分钟
    load_db_name = 'stat_query_50'
    load_coll_name = 'stat_xdr_in_'+tool_util.get_date(0)
    load_speed = load_stat_query.query(mongo_client, load_db_name, load_coll_name, start_time, end_time, period)
    result['sum' + str(index + 3)] = round(load_speed, 2)

    # TODO query service speed  次/分钟
    service_coll_name = 'stat_service'
    service_speed = service_stat_query.query(mongo_client, db_name, service_coll_name, start_time, end_time, period)
    result['sum' + str(index + 4)] = round(service_speed, 2)

    # TODO combine result
    merger_infos_result = {"kpiCode": "MONGO_EMIT_STAT", "message": json.dumps(result)}
    # merger_infos_result = {"kpiCode": 'MONGO_EMIT_STAT', "message": json.dumps(result)}
    logging.info(f'merger_infos_result is {json.dumps(merger_infos_result)}')
    print(json.dumps(merger_infos_result))
    roboter.setContent(json.dumps(merger_infos_result))
