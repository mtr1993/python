# coding=utf-8
#########################################
# Function:    statistics: the number of applications per user
# Usage:       apply/python robot/patrol.py -i task.cfg debug
# Author:      AIHC DEV TEAM
# Company:     AsiaInfo Inc.
# Version:     1.0
#########################################

import json
from subprocess import Popen, PIPE

global roboter

if __name__ == '__main__':
    users = roboter.getConfigVal('exclude_user')
    comms = roboter.getConfigVal('exclude_comm')
    # users = "[\"root\",\"polkitd\",\"postfix\",\"rpc\",\"dbus\",\"chrony\"]"
    # comms = "[\"sh\",\"bash\",\"grep\",\"ps\"]"
    exclude_user = json.loads(users)
    exclude_comm = json.loads(comms)
    print('exclude_user is '+str(exclude_user)+' ,exclude_comm is '+str(exclude_comm))

    #ps -eo user,pcpu,pmem,rss,comm --sort user|grep -v root
    pipe = Popen('ps -eo user,pcpu,pmem,rss,comm --sort user|grep -v root', shell=True, stdout=PIPE, close_fds=True)
    list = pipe.communicate()[0].rstrip('\n').split('\n')
    print(list)

    process = {}
    for i in range(1, len(list)):
        record = list[i].split()
        user = record[0].replace('+', '')
        pcpu = float(record[1])
        pmem = float(record[2])
        smem = int(record[3])
        comm = record[4]

        if exclude_user.count(user) > 0:
            continue

        if exclude_comm.count(comm) > 0:
            continue

        if process.get(user) is None:
            process[user] = {}
            process[user][comm] = {'number': 1, 'pcpu': pcpu, 'pmem': pmem, 'smem': smem}
        else:
            if process.get(user).get(comm) is None:
                process[user][comm] = {'number': 1, 'pcpu': pcpu, 'pmem': pmem, 'smem': smem}
            else:
                process[user][comm]['number'] = process.get(user).get(comm).get('number') + 1
                process[user][comm]['pcpu'] = process.get(user).get(comm).get('pcpu') + pcpu
                process[user][comm]['pmem'] = process.get(user).get(comm).get('pmem') + pmem
                process[user][comm]['smem'] = process.get(user).get(comm).get('smem') + smem

    result = []
    for user in process:
        item = {}
        item['user'] = user
        item['apps'] = []

        apps = process.get(user)
        for app in apps:
            info = apps.get(app)
            item['apps'].append({'name': app, 'number': info.get('number'), 'pcpu': round(info.get('pcpu'), 1),
                                 'pmem': round(info.get('pmem'), 1), 'smem': info.get('smem')})

        result.append(item)
    roboter.setContent(json.dumps(result))
    """
    {"apps": [{"pcpu": 2.4, "pmem": 11.0, "name": "mongod", "smem": 3689260, "number": 4}, 
    {"pcpu": 0.1, "pmem": 0.0, "name": "mongos", "smem": 20876, "number": 1}], 
    "user": "mongo_ln"}
    """