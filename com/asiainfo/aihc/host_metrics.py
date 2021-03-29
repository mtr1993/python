#########################################
# Function:    sample linux performance indices
# Usage:       python sampler.py
# Author:      AIHC DEV TEAM
# Company:     AsiaInfo Inc.
# Version:     2.2
#########################################

import os
import os.path
import sys
import time
import datetime
import operator
import httplib
import logging
import socket
import random
import json
from shutil import copyfile
from subprocess import Popen, PIPE
from logging.handlers import RotatingFileHandler

# import ksql

global logger
# REMOTE_HOST = None
# REMOTE_PORT = None
# REMOTE_MONITOR_URI = None
# UUID = None
global roboter


def getIoFilter():
    disks = roboter.getConfigVal("disks")
    if disks:
        arr = disks.split(',')
        str = "\"||$1==\"".join(arr)
        str = "awk 'NULL==\"" + str + "\"'"
        return str
    else:
        return None


def getNetWorks():
    networks = roboter.getConfigVal("ethernet")
    if networks:
        return networks.split(',')
    else:
        return None


# IFNAME = roboter.getConfigVal("ethernet")
IO_FILTER = getIoFilter()


# IO_FILTER = "awk 'NULL==\"sda\"'"

def get_disk_partition():
    fileSystems = roboter.getConfigVal("fileSystems").split(',')
    if fileSystems:
        return fileSystems
    else:
        return None


def get_mem_usage_percent():
    f = open('/proc/meminfo', 'r')
    for line in f:
        if line.startswith('MemTotal:'):
            mem_total = int(line.split()[1])
        elif line.startswith('MemFree:'):
            mem_free = int(line.split()[1])
        elif line.startswith('Buffers:'):
            mem_buffer = int(line.split()[1])
        elif line.startswith('Cached:'):
            mem_cache = int(line.split()[1])
        elif line.startswith('SwapTotal:'):
            m_total = int(line.split()[1])
        elif line.startswith('SwapFree:'):
            m_free = int(line.split()[1])
        else:
            continue
    f.close()
    physical_percent = usage_percent(mem_total - (mem_free + mem_buffer + mem_cache), mem_total)
    virtual_percent = 0
    if m_total > 0:
        virtual_percent = usage_percent((m_total - m_free), m_total)
    return physical_percent, virtual_percent, int(usage_unit(mem_total, 2)), usage_unit(
        mem_total - mem_free - mem_buffer - mem_cache, 2)


black_list = ('iso9660',)


def usage_unit(use, times):
    try:
        division = 1024
        count = 1
        while (count < times):
            count = count + 1
            division = division * 1024
        ret = use / division
    except ZeroDivisionError:
        raise Exception("ERROR - zero division error")
    return float('%0.2f' % ret)


def usage_percent(use, total):
    try:
        ret = (float(use) / total) * 100
    except ZeroDivisionError:
        raise Exception("ERROR - zero division error")
    return float('%0.2f' % ret)


def check_disk():
    try:
        return_dict = {}
        p_list = get_disk_partition()

        if not p_list:
            return None

        for i in p_list:
            dt = os.statvfs(i)
            use = (dt.f_blocks - dt.f_bfree) * dt.f_frsize
            all = dt.f_blocks * dt.f_frsize
            return_dict[i] = ('%.2f' % (usage_percent(use, all),), ('%.2f' % (all * 1.0 / (1024 * 1000000))))
    except:
        return None
    return return_dict


_CLOCK_TICKS = os.sysconf("SC_CLK_TCK")


def get_cpu_time():
    need_sleep = True
    if not os.path.isfile('../tmp/cpu_stat') or os.path.getsize('../tmp/cpu_stat') == 0:
        copyfile('/proc/stat', '../tmp/cpu_stat')
        need_sleep = True

    f1 = open('../tmp/cpu_stat', 'r')
    values1 = f1.readline().split()
    total_time1 = 0
    for i in values1[1:]:
        total_time1 += int(i)
    idle_time1 = int(values1[4])
    iowait_time1 = int(values1[5])
    f1.close()

    if need_sleep:
        time.sleep(1)

    f2 = open('/proc/stat', 'r')

    values2 = f2.readline().split()
    total_time2 = 0
    for i in values2[1:]:
        total_time2 += int(i)
    idle_time2 = int(values2[4])
    iowait_time2 = int(values2[5])

    f2.close()

    idle_time = idle_time2 - idle_time1
    iowait_time = iowait_time2 - iowait_time1
    total_time = total_time2 - total_time1

    cpu_percentage = int(100.0 * (total_time - idle_time - iowait_time) / total_time)
    # compensate logic
    if total_time < 0 or idle_time < 0 or iowait_time < 0 or cpu_percentage < 0 or cpu_percentage > 100:
        time.sleep(1)
        f3 = open('/proc/stat', 'r')

        values3 = f3.readline().split()
        total_time3 = 0
        for i in values3[1:]:
            total_time3 += int(i)
        idle_time3 = int(values3[4])
        iowait_time3 = int(values3[5])

        f3.close()

        idle_time = idle_time3 - idle_time2
        iowait_time = iowait_time3 - iowait_time2
        total_time = total_time3 - total_time2
        cpu_percentage = int(100.0 * (total_time - idle_time - iowait_time) / total_time)

    copyfile('/proc/stat', '../tmp/cpu_stat')
    return cpu_percentage


def network_io_kbitps():
    """Return network I/O statistics for every network interface
    installed on the system as a dict of raw tuples.

    """
    list = getNetWorks()
    if not list:
        return None
    f1 = open("/proc/net/dev", "r")
    lines1 = f1.readlines()
    f1.close()

    # list = ['bond0', 'bond1', 'lo']
    retdict1 = {}
    for line1 in lines1[2:]:
        colon1 = line1.find(':')
        assert colon1 > 0, line1
        name1 = line1[:colon1].strip()
        if list.count(name1):
            fields1 = line1[colon1 + 1:].strip().split()
            bytes_recv1 = float('%.4f' % (float(fields1[0]) * 0.0078125))
            bytes_sent1 = float('%.4f' % (float(fields1[8]) * 0.0078125))
            retdict1[name1] = (bytes_recv1, bytes_sent1)

    time.sleep(1)
    f2 = open("/proc/net/dev", "r")
    lines2 = f2.readlines()
    f2.close()
    retdict2 = {}
    for line2 in lines2[2:]:
        colon2 = line2.find(':')
        assert colon2 > 0, line2
        name2 = line2[:colon2].strip()
        if list.count(name2):
            fields2 = line2[colon2 + 1:].strip().split()
            bytes_recv2 = float('%.4f' % (float(fields2[0]) * 0.0078125))
            bytes_sent2 = float('%.4f' % (float(fields2[8]) * 0.0078125))
            retdict2[name2] = (bytes_recv2, bytes_sent2)

    retdict = merge_with(retdict2, retdict1)
    return retdict


def disk_io_Kbps():
    if not IO_FILTER:
        return None
    iostat = Popen("iostat -d -k 1 2 | sed '/Device\|Linux\|^$/d' |" + IO_FILTER + "> ../tmp/disk_io", shell=True,
                   stdout=PIPE, stderr=PIPE, close_fds=True)
    iostat_error = iostat.communicate()[1].strip()
    if iostat_error:
        logger.error("iostat not exists, %s" % iostat_error)
        return None

    retdict = {}
    f = open('../tmp/disk_io', 'r')
    lines = f.readlines()
    for line in lines:
        name, _, readkps, writekps, _, _, = line.split()
        if name:
            readkps = float(readkps)
            writekps = float(writekps)
            retdict[name] = (readkps, writekps)
    f.close()
    return retdict


def merge_with(d1, d2, fn=lambda x, y: tuple(map(operator.sub, x, y))):
    res = d1.copy()  # "= dict(d1)" for lists of tuples
    for key, val in d2.iteritems():  # ".. in d2" for lists of tuples
        try:
            res[key] = fn(res[key], val)
        except KeyError:
            res[key] = val
    return res


def get_load():
    try:
        f = open('/proc/loadavg', 'r')
        tmp = f.readline().split()
        lavg_1 = float(tmp[0])
        lavg_5 = float(tmp[1])
        lavg_15 = float(tmp[2])
        f.close()
    except:
        return None
    return lavg_1, lavg_5, lavg_15


def get_tcp_status():
    check_cmd = "command -v ss"
    check_proc = Popen(check_cmd, shell=True, stdout=PIPE, close_fds=True)
    ss = check_proc.communicate()[0].rstrip('\n')
    if ss:
        cmd = "ss -ant | awk '{if(NR != 1) print NULL}' | awk '{state=NULL;arr[state]++} END{for(i in arr){printf \"%s=%s \", i,arr[i]}}' | sed 's/-/_/g' | sed 's/ESTAB=/ESTABLISHED=/g' | sed 's/FIN_WAIT_/FIN_WAIT/g'"
    else:
        cmd = "netstat -anp | grep tcp | awk '{print NULL}' | awk '{state=NULL;arr[state]++} END{for(i in arr){printf \"%s=%s \", i,arr[i]}}' | tail -n 1"
    tcp_proc = Popen(cmd, shell=True, stdout=PIPE, close_fds=True)
    tcp_status = tcp_proc.communicate()[0].rstrip('\n')
    return tcp_status


def get_proc_number():
    cmd = "ps axu | wc -l | tail -n 1"
    proc_func = Popen(cmd, shell=True, stdout=PIPE, close_fds=True)
    proc_number = proc_func.communicate()[0].rstrip('\n')
    return proc_number


def all_index():
    return (
        int(time.time() * 1000),
        get_cpu_time(),
        get_mem_usage_percent(),
        check_disk(),
        disk_io_Kbps(),
        network_io_kbitps(),
        get_load(),
        # get_tcp_status(),
        get_proc_number()
    )


def get_local_ip(ifname):
    import fcntl, struct
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    inet = fcntl.ioctl(s.fileno(), 0x8915, struct.pack('256s', ifname[:15]))
    return socket.inet_ntoa(inet[20:24])


def get_cpu_forecast():
    # node_id = roboter.getNodeId()
    node_id = '167898152'
    threeDayAgo = (datetime.datetime.now() - datetime.timedelta(days=3))
    timeStamp = int(time.mktime(threeDayAgo.timetuple()) * 1000)
    client = ksql.KSQLAPI('http://10.1.236.40:8099')
    query = client.query(
        "select TIMESTAMP,FORECAST,STDDEV from PM_HOST_CPU_PREDICTED where ROWKEY = '%s' and Windowstart >= %s" % (
        node_id, timeStamp))
    query_result = ''
    for items in query:
        query_result = query_result + items

    results = json.loads(query_result)
    forecast_result = {}
    for item in results:
        if item.has_key('row'):
            result = item.values()
            forecast_result["timestamp"] = result[0]['columns'][0]
            forecast_result["forecast"] = result[0]['columns'][1]
            f = result[0]['columns'][2]
            a = '%.1f' % f
            forecast_result["stddev"] = a

    return forecast_result


def collector():
    timestamp, cpu, mem, disk, disk_io, net, load, process_number = all_index()
    disk_utilization = ''
    disk_io_read = ''
    disk_io_write = ''
    internet_networkrx = ''
    internet_networktx = ''
    tcp_status_count = ''
    tcp_status = None
    period_1 = ''
    period_5 = ''
    period_15 = ''
    symbole = '\n'

    is_forecast = roboter.getConfigVal("is_forecast")

    if is_forecast == '':
        is_forecast = 0

    if is_forecast == '1':
        cpu_forecast = {}
        cpu_forecast = {"dimension": "CPU_FORECAST", "kpi_id": "9011", "value": json.dumps(get_cpu_forecast())}

    # cpu_utilization = 'CPUUtilization ' + str(timestamp) + ' ' + str(cpu) + ' Percent CPU\n'
    cpu_utilization = {"dimension": "CPU", "kpi_id": "9001", "value": str(cpu)}
    # cpu_user = {"dimension":"AvgUser","name":"CPUDetail","value":"20"}
    # cpu_sys = {"dimension":"AvgSystem","name":"CPUDetail","value":"10"}
    # memory_utilization = 'MemoryUtilization ' + str(timestamp) + ' ' + str(mem[0]) + ' Percent Memory\n'
    memory_utilization = {"dimension": "Memory", "kpi_id": "9002", "value": str(mem[0])}
    # memory_total = {"dimension":"MemoryTotal","name":"MemoryDetail","value":str(mem[2]),"unit":"GB"}
    # memory_use = {"dimension":"MemoryConsts","name":"MemoryDetail","value":str(mem[3]),"unit":"GB"}
    if load:
        # period_1 = 'LoadAverage ' + str(timestamp) + ' ' + str(load[0]) + ' count 1min\n'
        # period_5 = 'LoadAverage ' + str(timestamp) + ' ' + str(load[1]) + ' count 5min\n'
        # period_15 = 'LoadAverage ' + str(timestamp) + ' ' + str(load[2]) + ' count 15min\n'
        period_1_temp = {"dimension": "1min", "kpi_id": "9004", "value": str(load[0])}
        period_1 = json.dumps(period_1_temp) + ','
        period_5_temp = {"dimension": "5min", "kpi_id": "9004", "value": str(load[1])}
        period_5 = json.dumps(period_5_temp) + ','
        period_15_temp = {"dimension": "15min", "kpi_id": "9004", "value": str(load[2])}
        period_15 = json.dumps(period_15_temp) + ','
    if disk:
        for name, value in disk.items():
            disk_tmp = {"kpi_id": "9003", "dimension": name, "value": str(value[0])}
            disk_utilization = disk_utilization + json.dumps(disk_tmp) + ','

    if disk_io:
        for name, value in disk_io.items():
            disk_io_read_tmp = {"kpi_id": "9005", "dimension": name, "value": str(round(float(value[0]) / 1024, 2))}
            disk_io_write_tmp = {"kpi_id": "9006", "dimension": name, "value": str(round(float(value[1]) / 1024, 2))}
            disk_io_read = disk_io_read + json.dumps(disk_io_read_tmp) + ','
            disk_io_write = disk_io_write + json.dumps(disk_io_write_tmp) + ','
    if net:
        for name, value in net.items():
            internet_networkrx_tmp = {"kpi_id": "9007", "dimension": name,
                                      "value": str(round(float(value[0]) / 1024, 2))}
            internet_networktx_tmp = {"kpi_id": "9008", "dimension": name,
                                      "value": str(round(float(value[1]) / 1024, 2))}
            internet_networkrx = internet_networkrx + json.dumps(internet_networkrx_tmp) + ','
            internet_networktx = internet_networktx + json.dumps(internet_networktx_tmp) + ','

    if tcp_status:
        status_count = tcp_status.split()
        for element in status_count:
            key_value = element.split('=')
            tcp_status_count_tmp = {"kpi_id": "9009", "dimension": key_value[0], "value": key_value[1]}
            tcp_status_count = tcp_status_count + json.dumps(tcp_status_count_tmp) + ','

    # process_count = 'ProcessCount ' + str(timestamp) + ' ' + process_number + ' Count Process\n'
    process_count = {"kpi_id": "9010", "dimension": "Process", "value": process_number}
    data_post = json.dumps(cpu_utilization) + ',\n' + json.dumps(
        memory_utilization) + ',\n' + period_1 + symbole + period_5 + symbole + period_15 + symbole + disk_utilization + symbole + disk_io_read + symbole + disk_io_write + symbole + internet_networkrx + symbole + internet_networktx + symbole + tcp_status_count + symbole + json.dumps(
        process_count)

    instanceid = roboter.getNodeIp()
    nodeCode = roboter.getNodeId()
    headers = {"content_Type": "text/plain", "Accept": "text/plain", "hold": "300", "node_Code": nodeCode,
               "instance_Id": instanceid, "group": "Decoding", "time": str(timestamp)}

    header = '''{"header":''' + json.dumps(headers) + ''',"content":['''

    roboter.setContent(header + data_post + ''']}''')


if __name__ == '__main__':

    if not os.path.exists("../tmp"):
        os.mkdir("../tmp")

    collector()