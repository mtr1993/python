# coding=utf-8

import json
from subprocess import Popen, PIPE

global roboter

# [
# [{'ip': '169039137', 'apps': {'load': '1', 'mongod': '30', 'recv': '0', 'emit': '0', 'service': '0'}}],
# [{'ip': '169039138', 'apps': {'load': '0', 'mongod': '26', 'recv': '0', 'emit': '0', 'service': '2'}}],
# [{'ip': '169039139', 'apps': {'load': '0', 'mongod': '32', 'recv': '4', 'emit': '0', 'service': '2'}}]
#  ]

if __name__ == '__main__':
    function_name_list = ['emit', 'recv', 'load', 'service', 'mongod']
    function_number_dic = {'ip': '10.19.85.33'}
    cmd_example = "ps -ef|grep fun|grep -v grep |wc -l"
    print('function_name_list is ' + str(function_name_list))
    for fun in function_name_list:
        cmd_str = cmd_example.replace("fun", fun)
        pipe = Popen(cmd_str, shell=True, stdout=PIPE, close_fds=True)
        number = pipe.communicate()[0].decode().strip()
        function_number_dic[fun] = int(number)
        print('number is ' + number)
    # print('function_number_dic is '+function_number_dic)
    # pipe = Popen('ps -eo user,pcpu,pmem,rss,comm --sort user|grep -v root', shell=True, stdout=PIPE, close_fds=True)
    # list = pipe.communicate()[0].rstrip('\n').split('\n')
    # print(list)
    #
    # process = {}
    # for i in range(1, len(list)):
    #     record = list[i].split()
    #     user = record[0].replace('+', '')
    #     pcpu = float(record[1])
    #     pmem = float(record[2])
    #     smem = int(record[3])
    #     comm = record[4]
    #
    #     if exclude_user.count(user) > 0:
    #         continue
    #
    #     if exclude_comm.count(comm) > 0:
    #         continue
    #
    #     if process.get(user) is None:
    #         process[user] = {}
    #         process[user][comm] = {'number': 1, 'pcpu': pcpu, 'pmem': pmem, 'smem': smem}
    #     else:
    #         if process.get(user).get(comm) is None:
    #             process[user][comm] = {'number': 1, 'pcpu': pcpu, 'pmem': pmem, 'smem': smem}
    #         else:
    #             process[user][comm]['number'] = process.get(user).get(comm).get('number') + 1
    #             process[user][comm]['pcpu'] = process.get(user).get(comm).get('pcpu') + pcpu
    #             process[user][comm]['pmem'] = process.get(user).get(comm).get('pmem') + pmem
    #             process[user][comm]['smem'] = process.get(user).get(comm).get('smem') + smem
    #

#     result = []
#     item = {}
#     item['ip'] = "IP"
#     item['apps'] = function_number_dic
#
# result.append(item)
print(json.dumps(function_number_dic))
roboter.setContent(json.dumps(function_number_dic))
