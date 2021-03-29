import os
from decimal import Decimal

from doc.chapter import Chapter
from doc.document import Document

if __name__ == '__main__':
    doc_path = '/Users/mtr/PycharmProjects/mongoQuery/com/asiainfo/aihc/doc_1_20190807'
    document = Document(os.path.join(doc_path, 'host-report-tpl.docx'))
    chapter = Chapter(os.path.join(doc_path, 'host-mongo_query_apps-tpl.docx'))
    host_mongo_query_apps = [{'load': '1', 'mongod': '30', 'service': '0', 'ip': '169039137', 'recv': '0', 'emit': '0'}]
    #host_mongo_query_apps = [{'id': '169039137', 'ip': '10.19.85.33', 'project': 'mongo-33', 'usage': 'mongo/docker/aihc', 'cost': Decimal('748.00'), 'manager': '张大伟', 'opendate': '20190410', 'version': 'Description: Red Hat Enterprise Linux Server release 7.4 (Maipo)', 'core': 4, 'memory': 32, 'space': 200, 'area': '重批云', 'avg_cpu': 18.2, 'avg_memory': 89.1, 'max_cpu': 54.0, 'max_memory': 92.2, 'avg_process': 233, 'bg_memory': 'FF0000', 'bg_max_memory': 'FF0000', 'bg_cpu': 'FFFFFF', 'bg_max_cpu': 'FFFFFF', 'space_percent': 79.8, 'bg_space': 'FF9900'}]
    table = {'table': host_mongo_query_apps}
    chapter.add_context(table)
    print(f"host_mongo_query_apps table  is {table}")
    document.add_chapter(chapter, title="应用数量统计", level=2)
    document.render()
    document.save(os.path.join(doc_path, '%s云主机使用情况.docx' % ('mongo_query_app')))
