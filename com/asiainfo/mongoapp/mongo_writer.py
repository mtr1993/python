from pymongo import MongoClient
from pymongo import collection
import logging
import traceback
logging.basicConfig(level=logging.DEBUG)


class mongo_writer:

    def __init__(self) -> None:
        # mongo_cli = auth(usr, passwd, host_ip, port)
        # self.__mongo_client = mongo_cli
        super().__init__()


# 获取连接 collection
def get_conn(db_client, database, coll):
    db = db_client[database]
    coll_cli = db[coll]
    assert isinstance(coll_cli, collection.Collection)
    return coll_cli


# # 插入数据到MongoDB中--test
# def insertinfo_test(db_client, database, coll):
#     # 连接数据库对应的集合
#     # collection = dbClient.database.coll
#     db = db_client[database]
#     coll_cli = db[coll]
#
#     # 插入数据
#     count = 0
#     try:
#         while count < 10:
#             name = 'test' + str(count)
#             age = count
#             print('the name is: ', name, " age is: ", age)
#             result = coll_cli.insert_one({"name": name, "age": age})
#             print('insert  result is: ', result)
#             count += 1
#     except Exception:
#         traceback.print_exc()


# 插入数据到MongoDB中
def conn_insertone(db_client, database, coll, document):
    # 连接数据库对应的集合
    # collection = dbClient.database.coll
    db = db_client[database]
    coll_cli = db[coll]
    try:
        result = coll_cli.insert_one(document)
        logging.info(f'insert  result is: {result.inserted_id}')
    except Exception as e:
        logging.error(f"exception is: {e}")


# 插入数据到MongoDB中
def conn_insertmany(db_client, database, coll, document_list):
    # 连接数据库对应的集合
    # collection = dbClient.database.coll
    db = db_client[database]
    coll_cli = db[coll]
    assert isinstance(coll_cli, collection.Collection)
    try:
        result = coll_cli.insert_many(document_list)
        logging.info(f'insert  result is: {result.inserted_ids}')

    except Exception as e:
        logging.error(f"exception is: {e}")


# 删除MongoDB中数据
def conn_delete(db_client, database, coll, condition):
    # 连接数据库对应的集合
    # collection = dbClient.database.coll
    db = db_client[database]
    coll_cli = db[coll]

    try:
        result = coll_cli.delete_one(condition)
        logging.info(f'delete: {database}.{coll} result is: {result.deleted_count}')
    except Exception as e:
        logging.error(f"exception is: {e}")


# 更新MongoDB中数据 upsert == True 如果数据库中不存在condition条件数据  则insert 存在则更新
# upsert == False 如果数据库中存在condition条件数据 则更新 否则不做任何操作
def conn_update(db_client, database, coll, condition, new_values, upsert):
    # 连接数据库对应的集合
    # collection = dbClient.database.coll
    db = db_client[database]
    coll_cli = db[coll]

    try:
        result = coll_cli.update_one(condition, new_values, upsert)
        logging.info(f'update: {database}.{coll} condition is {condition} '
                     f' to  {new_values}   result is: {result.modified_count}')
    except Exception as e:
        traceback.print_exception(e)
        logging.error(f"exception is: {e}")


# 查询MongoDB中数据
def conn_query(db_client, database, coll, condition):
    # 连接数据库对应的集合
    # collection = dbClient.database.coll
    logging.debug(f' database is {database}, coll is {coll}, condition is {condition}')
    db = db_client[database]
    coll_cli = db[coll]
    result_list = []
    try:
        # 查询数据
        for i in coll_cli.find(condition):
            result_list.append(i)
    except Exception as e:
        logging.error(f"exception is: {e}")
    return result_list


def conn_aggregate(db_client, database, coll, condition):
    db = db_client[database]
    coll_cli = db[coll]
    result_list = []
    try:
        for i in coll_cli.aggregate(condition):
            result_list.append(i)
    except Exception as e:
        logging.error(f"exception is: {e}")
    return result_list


# 数据库认证 返回认证的客户端
def auth(usr, passwd, host_ip, port):
    mongo_client = MongoClient(host_ip, port)
    # 连接mongodb数据库,账号密码认证
    db_client = mongo_client.admin  # 先连接系统默认数据库admin
    # 进行密码认证  让admin数据库去认证密码登录
    db_client.authenticate(usr, passwd, mechanism='SCRAM-SHA-1')
    return mongo_client


# 释放数据库连接
def close_client(client):
    assert isinstance(client, MongoClient)
    try:
        if client is not None:
            client.close()
    except Exception as e:
        logging.error(f"exception is: {e}")
