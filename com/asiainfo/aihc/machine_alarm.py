import psycopg2
import time
import datetime
import json
import redis
import os
from publisher import *


global roboter
global logger


class Conf:
    hosts = roboter.domainParam('bhc_hosts')
    user = roboter.domainParam('bhc_user')
    port = int(roboter.domainParam('bhc_port'))
    password = roboter.decodeParam('bhc_passwd')
    database = roboter.domainParam('bhc_database')
    redis_password = roboter.decodeParam('engine_password')
    redis_port = int(roboter.domainParam('engine_port'))
    redis_ip = roboter.getServIp()
    serv_host = roboter.getServIp()
    serv_port = int(roboter.getServPort())
    node_ip = roboter.getNodeIp()
    node_id = roboter.getNodeId()
    region_id = roboter.getRegionId()

    script_file = 'machine_alarm.py'


def db_connect():
    try:
        conn = psycopg2.connect(host=Conf.hosts, port=Conf.port, database=Conf.database, user=Conf.user,
                                password=Conf.password, client_encoding="UTF-8")
    except Exception as ex:
        logger.info(ex)
    finally:
        return conn


def diff_time(redis_time, datatime):
    now_time = datetime.datetime.strptime(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()), "%Y-%m-%d %H:%M:%S")
    end_time = datetime.datetime.strptime(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(redis_time) / 1000)),
                                          "%Y-%m-%d %H:%M:%S")
    days = (now_time - end_time).days
    seconds = (now_time - end_time).seconds
    total_seconds = (now_time - end_time).total_seconds()
    return days, seconds, total_seconds


# def start_up(publisher,host_ip,db_connect):
#     executor = ScriptExecutor(publisher)
#     script_id,script_name,script_type,timeout = get_partol_script_info(Conf.script_file,'start_up',db_connect)
#     try:
#         executor.begin()

#         executor.script(script_id=script_id,
#                         script_name=script_name,
#                         script_type=script_type,
#                         timeout=timeout)
#         executor.change_destination(host.get('ip'),ip_addr=True).call()

def clean_disk(publisher, host_ip, db_connect):
    executor = ScriptExecutor(publisher)
    script_id, script_name, script_type, timeout = get_partol_script_info(Conf.script_file, 'clean_disk', db_connect)
    try:
        executor.begin()

        executor.script(script_id=script_id,
                        script_name=script_name,
                        script_type=script_type,
                        timeout=timeout)
        executor.change_destination(host_ip, ip_addr=True).call()

    except Exception as e:
        logger.info(e)


def get_partol_script_info(script_file, task_name, db_connect):
    if db_connect is None:
        logger.error("Error is occurred when db connecting.")
        return None

    try:
        cur = db_connect.cursor()
        sql = "select TASK_ID,TASK_TYPE, SCRIPT_TYPE, SCRIPT_FILE,TIME_OUT from pm_kpi_collect_task where PARENT_ID = (select task_id from pm_kpi_collect_task where script_file = '%s') and task_name = '%s'" % (
        script_file, task_name)
        cur.execute(sql)
        rows = cur.fetchall()
        if rows is None:
            return None

        for row in rows:
            script_id = row[0]
            script_name = row[3]
            script_type = row[2]
            timeout = int(row[4])
    except Exception as e:
        logger.info(e)
    finally:
        return script_id, script_name, script_type, timeout


if __name__ == '__main__':
    r = redis.Redis(host=Conf.redis_ip, port=Conf.redis_port, password=Conf.redis_password, decode_responses=True)
    db = db_connect()
    pub = Publisher()
    pub.connect(Conf.serv_host, Conf.serv_port, Conf.node_id, localhost=Conf.node_ip, connect_wait=True)
    try:
        cur = db.cursor()
        sql = "select NODE_CODE,HOST_ADDRESS as NODE_IP,DOMAIN_ID,NODE_STS from pm_host_daemon_node where DOMAIN_ID in (1,2,3,4,5)"

        cur.execute(sql)
        result = cur.fetchall()
        datatime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        if result is not None:
            for record in result:
                node_id = record[0]
                list = r.lrange(str(node_id), 0, 0)
                push_info = {}
                push_info['type'] = 'Equipment'
                push_info['id'] = 'MACHINE'
                push_info['object'] = record[1]
                push_info['additional'] = {}
                push_info['additional']['host_ip'] = record[1]
                push_info['additional']['domain_id'] = record[2]
                push_info['additional']['alarmTime'] = datatime
                if len(list) > 0:
                    redis_time = json.loads(list[0])['time']
                    days, seconds, total_seconds = diff_time(redis_time, datatime)
                else:
                    total_seconds = 100000
                # online 0:应用不在;1:告警;2:disk100%,应用在
                node_sts = record[3]

                if node_sts == 1 and total_seconds > 28800:
                    push_info['additional']['online'] = 2
                    push_info['additional']['DISK'] = 100
                elif node_sts == 0:

                    # start_up(pub,record[1],db)
                    # logger.info('++++++++++++')

                    push_info['additional']['online'] = 0

                else:
                    push_info['additional']['online'] = 1
                    if len(list) > 0:
                        point = json.loads(list[0])['point']
                        for item in point:
                            if item['kpiId'] == '9001':
                                push_info['additional']['CPU'] = float(item['value'])
                            elif item['kpiId'] == '9002':
                                push_info['additional']['Memory'] = float(item['value'])
                            elif item['kpiId'] == '9003':
                                push_info['additional']['DISK'] = float(item['value'])
                                push_info['additional']['PATH'] = item['dimension']
                            else:
                                continue
                        if push_info['additional']['DISK'] > 90:
                            clean_disk(pub, record[1], db)
                r.lpush('/queue/alarm/events', json.dumps(push_info))

    except Exception as ex:
        logger.info(ex)
    finally:
        if db is not None:
            db.close()
        pub.disconnect()