import datetime
import random
import time

from asiainfo.mongoapp.mongo import mongo_writer

if __name__ == '__main__':
    username = 'root'
    password = 'root'
    mongos_host = '10.19.85.33'
    mongos_port = 34000
    db_name = 'test'
    coll_name = 'stat_emit'
    client = mongo_writer.auth(username, password, mongos_host, mongos_port)
    while True:
        start_time = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        send_lines = random.randint(10000, 90000)
        doc = {"StartTime": start_time, "SendLines": send_lines}
        print(doc)
        mongo_writer.conn_insertone(client, db_name, coll_name, doc)

        total_lines = random.randint(10000, 90000)
        recv_coll_name = 'stat_recv'
        doc = {"StartTime": start_time, "TotalLines": total_lines}
        print(doc)
        mongo_writer.conn_insertone(client, db_name, recv_coll_name, doc)

        ReadXdrCount = random.randint(10000, 90000)
        load_db_name = 'stat_redo_65'
        load_coll_name = 'stat_xdr_in'
        doc = {"StartTime": start_time, "ReadXdrCount": ReadXdrCount}
        print(doc)
        mongo_writer.conn_insertone(client, load_db_name, load_coll_name, doc)

        service_coll_name = 'stat_service'
        QueryCount = random.randint(10000, 90000)
        start_time = datetime.datetime.strptime(start_time, '%Y%m%d%H%M%S').strftime('%Y%m%d %H:%M:%S')
        doc = {"StartTime": start_time, "QueryCount": QueryCount, "TotalUse": 10}
        print(doc)
        mongo_writer.conn_insertone(client, db_name, service_coll_name, doc)

        time.sleep(5)
