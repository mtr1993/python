import datetime
import logging
import os

from asiainfo.mongoapp.exception import exception_stat_query
from asiainfo.mongoapp.mongo import mongo_writer
from doc.chapter import Chapter
from doc.document import Document




if __name__ == '__main__':
    username = 'root'
    password = 'root'
    mongos_host = '10.19.85.33'
    mongos_port = 34000
    db_name = 'test'
    coll_name = 'stat_excep'
    period = 7200
    host_mongo_query_exceptions = {}
    excep_result = []
    client = mongo_writer.auth(username, password, mongos_host, mongos_port)
    end_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    start_time = (datetime.datetime.now() + datetime.timedelta(minutes=-period)).strftime('%Y-%m-%d %H:%M:%S')
    result_list = exception_stat_query.query_excep(client, db_name, coll_name, start_time, end_time, period)
    logging.info(f'result_list is {result_list}')
    if len(result_list) != 0:
        for doc in result_list:
            id_doc = doc.get("_id")
            result_doc = id_doc
            result_doc.update({'count': doc.get('count')})
            new_id = id_doc.get('Ip')+'—'+id_doc.get('FunctionName')
            result_doc.update({'_id': new_id})
            logging.info(f'result_doc is {result_doc}')

            if host_mongo_query_exceptions is not None and host_mongo_query_exceptions.__contains__(new_id):
                excep_doc = host_mongo_query_exceptions.get(new_id)
                context_old = excep_doc.get('context')
                context_new = result_doc.get('ExceptionType') + "," +str(result_doc.get('count'))
                excep_doc.update({'context': context_old+','+context_new})
                print(excep_doc)
                excep_doc.update({'sumCount': (result_doc.get('count')+excep_doc.get('sumCount'))})


            else :
                logging.info(' not none')
                result_doc.update({'context': result_doc.get('ExceptionType')+","+
                                                                           str(result_doc.get('count'))})
                result_doc.update({'sumCount': 1})
                host_mongo_query_exceptions.update({new_id : result_doc})
    # host_mongo_query_exceptions = sorted(host_mongo_query_exceptions.items(), key=lambda d: d[0])

    logging.info(f'host_mongo_query_exceptions is {host_mongo_query_exceptions}')
    # \\print(sorted(host_mongo_query_exceptions.items(), key=lambda d: d[0]))
    doc_path = '/Users/mtr/PycharmProjects/mongoQuery/com/asiainfo/aihc/doc_1_20190807'
    document = Document(os.path.join(doc_path, 'host-report-tpl.docx'))
    chapter = Chapter(os.path.join(doc_path, 'host-mongo_query_exceptions-tpl.docx'))

    # host_mongo_query_exceptions = [{'ip': '10.19.85.33', 'FunctionName': 'load', 'count': '10',
    #                                 'context': 'NullPointException:1,RuntimeException：3'},
    #                                {'ip': '10.19.85.34', 'FunctionName': 'serv', 'count': '20',
    #                                 'context': 'NullPointException:2,RuntimeException：6'}]
    excep_result = host_mongo_query_exceptions.values()
    logging.info(f'excep_result is {excep_result}')
    table = {'table': excep_result}
    chapter.add_context(table)
    print(f"host-mongo_query_exceptions table  is {table}")
    document.add_chapter(chapter, title="异常数量统计", level=2)
    document.render()
    document.save(os.path.join(doc_path, '%s云主机使用情况.docx' % ('mongo_query_exception')))

    host_mongo_query_exceptions = [{'ip': '10.19.85.35', 'FunctionName': 'load', 'count': '10',
                                    'context': 'NullPointException:1,RuntimeException：3'},
                                   {'ip': '10.19.85.34', 'FunctionName': 'serv', 'count': '20',
                                    'context': 'NullPointException:2,RuntimeException：6'},
                                   {'ip': '10.19.85.33', 'FunctionName': 'serv', 'count': '20',
                                    'context': 'NullPointException:2,RuntimeException：6'}
                                   ]
    students_by_score = sorted(host_mongo_query_exceptions, key=lambda x: x['ip'],reverse=False)
    print(students_by_score)