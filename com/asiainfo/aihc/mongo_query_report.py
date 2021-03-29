# coding=utf-8

import os
import psycopg2
import numpy
import json as JSON
import datetime

from jinja2 import Environment, FileSystemLoader

from doc.chapter import Chapter
from publisher import ScriptExecutor, Publisher
from doc.document import Document

from pyecharts import options as opts
from pyecharts.charts import Grid, Line, Pie
from pyecharts.commons import utils
from pyecharts.render import make_snapshot
from snapshot_phantomjs import snapshot

import paramiko

global roboter
global logger


class Conf:
    # hosts = roboter.domainParam('bhc_hosts')
    # user = roboter.domainParam('bhc_user')
    # port = int(roboter.domainParam('bhc_port'))
    # password = roboter.decodeParam('bhc_passwd')
    # database = roboter.domainParam('bhc_database')
    #
    # serv_host = roboter.getServIp()
    # serv_port = int(roboter.getServPort())
    # node_ip = roboter.getNodeIp()
    # node_id = roboter.getNodeId()
    # region_id = roboter.getRegionId()

    # script_file = roboter.getConfigVal('script_file')
    # domain_id = int(roboter.getConfigVal('domain_id'))
    # head_id = int(roboter.getConfigVal('head_id'))
    hosts = '10.19.85.33,10.19.85.34,10.19.85.35'
    user = 'aihc'
    port = 12688
    password = 'aihc'
    database = 'bhc'
    serv_host = '10.19.85.33'
    serv_port = '8899'
    node_ip = '10.19.85.33'
    node_id = '169039137'
    region_id = '1000'
    script_file = 'host_resource_word.py'
    domain_id = 6
    head_id = 6
    # 'aihc', 12688, 'aihc', 'bhc', '10.19.85.33', 8899, '10.19.85.33', \
    # , '1000', 'host_resource_word.py', 6,



def date_cycle():
    now = datetime.datetime.now().strftime('%Y/%m/%d')
    pro = (datetime.datetime.now() + datetime.timedelta(days=-30)).strftime('%Y/%m/%d')

    return ("%s - %s" % (pro, now))


def format_space_size(space):
    if space / 1024 > 1024:
        return "%.1fG" % (round(space / 1024 / 1024, 1))
    elif space / 1024 > 1:
        return "%.1fM" % (round(space / 1024, 1))
    else:
        return "%.1fK" % (round(space, 1))


def cell_background_color(value):
    if value > 80:
        return 'FF0000'
    elif value > 60:
        return 'FF9900'
    else:
        return 'FFFFFF'


def db_connect():
    try:
        conn = psycopg2.connect(host=Conf.hosts, port=Conf.port, database=Conf.database, user=Conf.user,
                                password=Conf.password, client_encoding="UTF-8")
    except Exception as ex:
        logger.info(ex)
    finally:
        return conn


def get_domain_hosts(connect, domain):
    hosts = []
    index = {}
    fee = 0

    cursor = connect.cursor()
    sql = "select * from (select a.node_code,a.host_address,IFNULL(b.project,''),IFNULL(b.usage,''),IFNULL(b.cost,0),IFNULL(b.manager,''),IFNULL(b.opendate,''),IFNULL (b.version,''),IFNULL(b.core,0),IFNULL(b.memory,0),IFNULL(b.space,0),b.area,a.DOMAIN_ID from pm_host_daemon_node a left join pm_host_property b on a.host_address = b. address order by a.node_code) c where c.node_code <> 'patrol' and c.DOMAIN_ID = %d" % (
        domain)
    cursor.execute(sql)

    result = cursor.fetchall()
    i = 0
    for row in result:
        hosts.append({
            'id': row[0],
            'ip': row[1],
            'project': row[2],
            'usage': row[3],
            'cost': row[4],
            'manager': row[5],
            'opendate': row[6],
            'version': row[7],
            'core': row[8],
            'memory': row[9] | 0,
            'space': row[10] | 0,
            'area': row[11]
        })

        index[row[0]] = i
        i = i + 1
        if row[4] is not None:
            fee = fee + int(row[4])

    cursor.close()

    return hosts, index, fee


def get_host_metrics(connect, host):
    metrics = {}
    datetime = []
    cpu = []
    memory = []
    process = []

    cursor = connect.cursor()
    sql = "select timestamp,week_content from pm_metrics_points_week where host_id='%s' and createtime > now() + '-30 day'" % (
        host['ip'])
    cursor.execute(sql)
    result = cursor.fetchall()

    for row in result:
        datetime.append(int(row[0]))

        content = JSON.loads(row[1])
        for record in content:
            kpiId = record['kpiId']

            try:
                if kpiId == '9001':
                    cpu.append(float(record['value']))
                elif kpiId == '9002':
                    memory.append(float(record['value']))
                elif kpiId == '9010':
                    process.append(int(record['value']))
            except Exception as e:
                pass

    metrics['datetime'] = datetime
    metrics['cpu'] = cpu
    metrics['memory'] = memory
    metrics['process'] = process

    cursor.close()

    avg_cpu = 0
    max_cpu = 0
    if len(cpu) > 0:
        avg_cpu = round(numpy.mean(cpu), 1)
        max_cpu = round(numpy.max(cpu), 1)

    avg_memory = 0
    max_memory = 0
    if len(memory) > 0:
        avg_memory = round(numpy.mean(memory), 1)
        max_memory = round(numpy.max(memory), 1)

    avg_process = 0
    if len(process) > 0:
        avg_process = int(numpy.mean(process))

    return metrics, avg_cpu, avg_memory, avg_process, max_cpu, max_memory


def get_domain_metrics(connect, hosts):
    data = {}
    for host in hosts:
        metrics, cpu, memory, process, max_cpu, max_memory = get_host_metrics(connect, host)
        data[host['ip']] = {}
        data[host['ip']]['metrics'] = metrics
        data[host['ip']]['avg_cpu'] = cpu
        data[host['ip']]['avg_memory'] = memory
        data[host['ip']]['avg_process'] = process

        host['avg_cpu'] = cpu
        host['avg_memory'] = memory
        host['max_cpu'] = max_cpu
        host['max_memory'] = max_memory
        host['avg_process'] = process
        host['bg_memory'] = cell_background_color(memory)
        host['bg_max_memory'] = cell_background_color(max_memory)
        host['bg_cpu'] = cell_background_color(cpu)
        host['bg_max_cpu'] = cell_background_color(max_cpu)

    return data


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
    finally:
        return script_id, script_name, script_type, timeout


def get_disk_space(publisher, hosts, index, db_connect):
    disk_space = {}

    executor = ScriptExecutor(publisher)

    script_id, script_name, script_type, timeout = get_partol_script_info(Conf.script_file, 'disk_space', db_connect)

    try:
        executor.begin()

        executor.script(script_id=script_id,
                        script_name=script_name,
                        script_type=script_type,
                        timeout=timeout)
        for host in hosts:
            executor.change_destination(host.get('ip'), ip_addr=True).call()

        messages = executor.fetchall()

        for message in messages:
            if message.is_signed():
                total_size = 0
                total_used = 0
                total_avail = 0
                info = {"table": []}
                try:
                    result = JSON.loads(message.get_body())
                except Exception as e:
                    print(message.get_body())
                    continue
                for i in range(1, len(result)):
                    source = result[i][0]
                    size = float(result[i][1])
                    used = float(result[i][2])
                    avail = float(result[i][3])
                    target = result[i][5]
                    percent = round(used / size * 100, 1)

                    total_size = total_size + size
                    total_used = total_used + used
                    total_avail = total_avail + avail

                    info['table'].append({"source": source,
                                          "size": format_space_size(size),
                                          "used": format_space_size(used),
                                          'avail': format_space_size(avail),
                                          "percent": percent,
                                          "target": target
                                          })
                percent = round(total_used / total_size * 100, 1)
                info['total_size'] = format_space_size(total_size)
                info['total_used'] = format_space_size(total_used)
                info['total_avail'] = format_space_size(total_avail)
                info['total_percent'] = percent
                id = message.get_header_value('performer')
                disk_space[id] = info
                if index.get(id) is not None:
                    hosts[index.get(id)]['space_percent'] = percent
                    hosts[index.get(id)]['bg_space'] = cell_background_color(percent)
            else:
                logger.error("unreachable:" + message.not_signed_id())
    except Exception as e:
        print(e)
    finally:
        executor.end()

    return disk_space


def get_host_apps(publisher, hosts, db_connect):
    host_apps = {}

    executor = ScriptExecutor(publisher)

    script_id, script_name, script_type, timeout = get_partol_script_info(Conf.script_file, 'host_apps', db_connect)

    try:
        executor.begin()

        executor.script(script_id=script_id,
                        script_name=script_name,
                        script_type=script_type,
                        timeout=timeout)
        for host in hosts:
            executor.change_destination(host.get('ip'), ip_addr=True).call()

        messages = executor.fetchall()

        for message in messages:
            if message is not None:
                try:
                    result = ''
                    result = JSON.loads(message.get_body())
                    host_apps[message.get_header_value('performer')] = result
                except Exception:
                    logger.info(message.get_headers())
                    logger.info(result)
                    continue

                # host_apps[message.get_header_value('performer')] = result
    finally:
        executor.end()

    return host_apps


def image_host_apps(doc_path, host_apps, host):
    if host_apps.get(host['id']) is None:
        return

    data_inner = []
    data_outer = []

    for i in host_apps.get(host['id']):
        number = 0
        for x in i.get('apps'):
            number = number + x.get('number')
            data_outer.append([x.get('name'), x.get('number')])

        data_inner.append(["★%s" % (i.get('user')), number])

    options = opts.InitOpts(js_host=os.path.join(os.getcwd(), "scripts/javascript/"),
                            animation_opts=opts.AnimationOpts(animation=False))
    pipe = (
        Pie(options)
            .add('', data_inner, radius=["0%", "30%"],
                 label_opts=opts.LabelOpts(formatter='{b}'))
            .add('', data_outer, radius=['55%', '80%'],
                 label_opts=opts.LabelOpts(formatter='{b}：{c} ({d}%)', font_size=13))
            .set_global_opts(legend_opts=opts.LegendOpts(orient='vertical', pos_left=0),
                             title_opts=opts.TitleOpts(title="应用分布", pos_left='center'))
    )
    make_snapshot(snapshot, pipe.render(os.path.join(doc_path, "render_%s.html" % (host['ip'].replace(".", "_")))),
                  os.path.join(doc_path, "host_app_%s.png" % (host['ip'].replace(".", "_"))), pixel_ratio=1, delay=1,
                  is_remove_html=True)


def image_host_resource(doc_path, hosts_metrics, host):
    options = opts.InitOpts(js_host=os.path.join(os.getcwd(), "scripts/javascript/"),
                            animation_opts=opts.AnimationOpts(animation=False))
    line1 = (
        Line(options)
            .add_xaxis(hosts_metrics[host['ip']]['metrics']['datetime'])
            .add_yaxis("CPU", hosts_metrics[host['ip']]['metrics']['cpu'], is_symbol_show=False, is_smooth=True,
                       areastyle_opts=opts.AreaStyleOpts(color=utils.JsCode(
                           "new echarts.graphic.LinearGradient(0,0,0,1,[{offset:0,color:'rgb(255,158,68)'},{offset:1,color:'rgb(255,70,131)'}])"),
                           opacity=0.5))
            .add_yaxis("Memory", hosts_metrics[host['ip']]['metrics']['memory'], is_symbol_show=False, is_smooth=True)
            .set_series_opts(linestyle_opts=opts.LineStyleOpts(width=2))
            .set_global_opts(
            title_opts=opts.TitleOpts(title="主机资源使用率 (%s)" % (host['ip']),
                                      title_textstyle_opts=opts.TextStyleOpts(font_size=13),
                                      subtitle=" 数据来源-健康度平台"),
            xaxis_opts=opts.AxisOpts(type_="time"),
            yaxis_opts=opts.AxisOpts(
                axislabel_opts=opts.LabelOpts(formatter=utils.JsCode("function(val){return val + '%';}")))
        )
    )

    line2 = (
        Line(options)
            .add_xaxis(hosts_metrics[host['ip']]['metrics']['datetime'])
            .add_yaxis("Process", hosts_metrics[host['ip']]['metrics']['process'], is_symbol_show=False, is_smooth=True,
                       areastyle_opts=opts.AreaStyleOpts(color=utils.JsCode(
                           "new echarts.graphic.LinearGradient(0,0,0,1,[{offset:0,color:'#fff'},{offset:1,color:'#61a0a8'}])"),
                           opacity=0.5)
                       )
            .set_series_opts(linestyle_opts=opts.LineStyleOpts(width=2))
            .set_global_opts(
            xaxis_opts=opts.AxisOpts(type_="time", position="top", axislabel_opts=opts.LabelOpts(is_show=False)),
            yaxis_opts=opts.AxisOpts(is_inverse=True),
            title_opts=opts.TitleOpts(title="应用运行数量(所有)", pos_bottom="0%",
                                      title_textstyle_opts=opts.TextStyleOpts(font_size=13)),
            legend_opts=opts.LegendOpts(pos_bottom="0%")
        )
    )

    grid = (
        Grid(options)
            .add(line1, grid_opts=opts.GridOpts(pos_top=50, pos_left=50, pos_right=50, pos_bottom="50%"))
            .add(line2, grid_opts=opts.GridOpts(pos_top="58%", pos_left=50, pos_right=50, pos_bottom=30))
    )

    make_snapshot(snapshot, grid.render(os.path.join(doc_path, "render_%s.html" % (host['ip'].replace(".", "_")))),
                  os.path.join(doc_path, "host_res_%s.png" % (host['ip'].replace(".", "_"))), pixel_ratio=1, delay=1,
                  is_remove_html=True)


def get_head_title(connect, head_id):
    cursor = connect.cursor()
    sql = "select head_title,download_url,head_name from head_title where head_id = %s" % (head_id)
    cursor.execute(sql)
    results = cursor.fetchall()

    # global  head_title

    for result in results:
        head_title = result[0]
        download_url = result[1]
        head_name = result[2]

    return head_title, download_url, head_name


def upload_file(ip, port, username, password, local, remote):
    # 上传目录 or 文件
    paramiko.util.log_to_file("paramiko.log")
    trans = paramiko.Transport((ip, int(port)))
    trans.connect(username=username, password=password)
    sftp = paramiko.SFTPClient.from_transport(trans)

    try:
        remote_dir, remote_filename = os.path.split(remote)
        local_dir, local_filename = os.path.split(local)
        # remote中没有目标文件名,使用local_filename
        if remote_filename == '' or remote_filename == '.':
            remote_filename = local_filename
        # remote 中没有目标目录,使用默认目录
        if remote_dir == '':
            remote_dir = '.'
        try:

            sftp.chdir(remote_dir)  # 切换到目标目录

            sftp.put(local, remote_filename)
        except Exception as e:
            print(e, remote_dir, 'is not exists')
    except Exception as e:
        print(e, 'put', local, 'to', remote, 'fail')
    trans.close()


def html_table(hosts, add_head, cost, date_cycle, download_url):
    print(download_url)
    env = Environment(loader=FileSystemLoader('./doc_1_20190807'))
    template = env.get_template('template.html')
    html_file_name = add_head + "明细总览.html"
    print(2)
    with open('./doc_1_20190807/' + html_file_name, 'w+') as fout:
        html_content = template.render(hosts=hosts, add_head=add_head, fee=cost, date_cycle=date_cycle,
                                       download_url=download_url)
        fout.write(html_content)


if __name__ == '__main__':
    doc_path = '/Users/mtr/PycharmProjects/mongoQuery/com/asiainfo/aihc/doc_1_20190807'
    doc_id = 1

    # sftp_ip = '10.19.85.33'
    # sftp_port = 22022
    # sftp_username = 'aihc'
    # sftp_password = 'aihc@123'
    sftp_ip = '10.19.85.27'
    sftp_port = 22022
    sftp_username = 'maintain'
    sftp_password = 'maintain'
    pub = Publisher()
    pub.connect(Conf.serv_host, Conf.serv_port, Conf.node_id, localhost=Conf.node_ip, connect_wait=True)
    try:
        date_cycle = date_cycle()
        print(f'date_cycle is {date_cycle}')
        db_connect = db_connect()
        # TODO 获取主机ip 使用费用等信息
        hosts, index, cost = get_domain_hosts(db_connect, Conf.domain_id)
        print(f'hosts is {hosts}, index is {index}, cost is {cost}')
        # TODO 获取表的 标题头 下载url  head_name
        head_title, download_url, add_head = get_head_title(db_connect, Conf.head_id)
        print(f'head_title is {head_title},download_url is {download_url}, add_head is {add_head}')
        # # TODO 获取所有ip的主机信息  cpu memory process
        hosts_metrics = get_domain_metrics(db_connect, hosts)
        print(f'hosts_metrics is {hosts_metrics}')

        # TODO 获取磁盘信息
        # disk_space = get_disk_space(pub, hosts, index, db_connect)
        # print(f'disk_space is {disk_space}')
        disk_space = None
        # TODO 获取应用信息
        host_apps = get_host_apps(pub, hosts, db_connect)
        print(f'host_apps is {host_apps}')
        host_apps = None
        path = os.path.join(doc_path, 'host-report-tpl.docx')
        print(f'path is {path}')
        document = Document(os.path.join(doc_path, 'host-report-tpl.docx'))
        document.add_heading(add_head)

        chapter = Chapter(os.path.join(doc_path, 'host-summary-tpl.docx'))
        chapter.add_context({'date_cycle': date_cycle, 'table': hosts, 'fee': cost})
        document.add_chapter(chapter, title="主机明细总览", level=2)

        html_table(hosts, add_head, cost, date_cycle, download_url)

        for host in hosts:

            image_host_resource(doc_path, hosts_metrics, host)

            image_host_apps(doc_path, host_apps, host)

            ip = host['ip']
            id = host['id']

            document.add_heading("主机%s[%s]" % (ip, host['project']), level=2)

            chapter = Chapter(os.path.join(doc_path, 'host-property-tpl.docx'))
            chapter.add_context({'host': host})

            document.add_chapter(chapter, title="服务及成本", level=3)

            chapter = Chapter(os.path.join(doc_path, 'host-resource-tpl.docx'))
            chapter.add_context({'date_cycle': date_cycle,
                                 'app_num': hosts_metrics[ip]['avg_process'],
                                 'cpu_percent': hosts_metrics[ip]['avg_cpu'],
                                 'mem_percent': hosts_metrics[ip]['avg_memory']})

            image = "host_res_%s.png" % (ip.replace(".", "_"))
            chapter.add_picture('image', os.path.join(doc_path, image), width=240)
            document.add_chapter(chapter, title="资源使用率", level=3)

            # disk_info = disk_space.get(id)
            # if disk_info is not None:
            #     chapter = Chapter(os.path.join(doc_path, 'host-disk-tpl.docx'))
            #     chapter.add_context({"table": disk_info.get("table"),
            #                          "total_size": disk_info.get("total_size"),
            #                          "total_used": disk_info.get("total_used"),
            #                          "total_avail": disk_info.get("total_avail"),
            #                          "total_percent": disk_info.get("total_percent"),
            #                          })
            #
            #     document.add_chapter(chapter, title="磁盘空间", level=3)

            app_info = host_apps.get(id)
            if app_info is not None:
                chapter = Chapter(os.path.join(doc_path, 'host-apps-tpl.docx'))

                image = "host_app_%s.png" % (ip.replace(".", "_"))

                chapter.add_context({"table": app_info})
                chapter.add_picture('image', os.path.join(doc_path, image), width=240)

                document.add_chapter(chapter, title="应用统计", level=3)

        document.add_context(
            {'head_title': head_title, 'doc_date': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')})

        document.render()
        document.save(os.path.join(doc_path, '%s云主机使用情况.docx' % (add_head)))

        file_name = '%s云主机使用情况.docx' % (add_head)
        local = os.path.join('/Users/mtr/PycharmProjects/mongoQuery/com/asiainfo/aihc/doc_1_20190807', file_name)
    #     # 远程下载文件主机目录配置
    #     remote = os.path.join('/home/maintain/mongoapp/aihc', file_name)
    #
    #     upload_file(sftp_ip, sftp_port, sftp_username, sftp_password, local, remote)
    #
    finally:
        db_connect.close()
        pub.disconnect()
