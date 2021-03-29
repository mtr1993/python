import datetime
import fcntl
import json as JSON
import logging
import os
import socket
import struct
import time

from asiainfo.mongoapp.tool import tool_util


def get_local_ip(ifname):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    inet = fcntl.ioctl(s.fileno(), 0x8915, struct.pack('256s', ifname[:15]))
    print(f'inet is {inet}')
    return socket.inet_ntoa(inet[20:24])


if __name__ == '__main__':

    now_time = time.time()
    print(f'nowtime is {now_time}')
    file_path = '/Users/mtr/PycharmProjects/mongoQuery/resource/exception'
    for file_name in os.listdir(file_path):
        full_name = os.path.join(file_path, file_name)
        file_time = os.path.getmtime(full_name)
        print(f' full_name is {full_name}, filetime is {file_time}')
        print(now_time > file_time)

    today = datetime.datetime.now()
    offset = datetime.timedelta(days=-10)
    date = (today + offset).strftime('%Y-%m-%d %H:%M:%S')
    print('===='+date)


    dt = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(dt)
    ts = int(time.mktime(time.strptime(dt, "%Y-%m-%d %H:%M:%S")))
    print(ts)

    print(tool_util.get_date_mills(1))
    print(tool_util.get_date(0))

    print(tool_util.get_ip())

    hostname = socket.gethostname()
    print(hostname)
    sysinfo = socket.gethostbyname_ex(hostname)
    print(sysinfo)
    # 获取本机IP
    ip = socket.gethostbyname(hostname)
    # logging.debug(ip)

    # print("server ip:", get_local_ip('en0'))

    result = (round(10000000 / 1024 / 1024, 1))
    print(result)

    now_day = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    yesterday = (datetime.datetime.now() + datetime.timedelta(days=-1)).strftime('%Y-%m-%d %H:%M:%S')

    print(str(yesterday)+"-"+str(now_day))



