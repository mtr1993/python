import time
import json
import psycopg2

global roboter
global logger


def getMergerInfo(hosts, user, password, databases, port, kpiCode):
    merger_infos_list = []
    merger_infos_result = {}
    reslut = {}
    db = None
    for host in hosts.split(","):
        try:
            db = psycopg2.connect(host=host, database=databases, user=user, password=password, port=port,
                                  client_encoding="UTF-8")
            if db is not None:
                break
        except Exception:
            continue

    try:
        cur = db.cursor()
        # case1
        for i in range(0, 5):
            merger_sql = "select floor( random() * 1000)"
            cur.execute(merger_sql)
            merger_infos = cur.fetchall()
            for every_info in merger_infos:
                sum = every_info[0]
                index = 'sum' + str(i)
                reslut[index] = int(sum)

        merger_infos_result["kpiCode"] = kpiCode
        merger_infos_result["message"] = json.dumps(reslut)

    except Exception :
        return None
    finally:
        if db is not None:
            db.close()
    return merger_infos_result


if __name__ == '__main__':
    host = '10.19.85.33'
    port = 12688
    databases = 'bhc'
    user = 'aihc'
    password = 'aihc'
    kpiCode = 'MONGO_EMIT_STAT'

    result = getMergerInfo(host, user, password, databases, port, kpiCode)
    print(json.dumps(result))
    # roboter.setContent(json.dumps(result))