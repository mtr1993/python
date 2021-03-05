from asiainfo.mongoapp.mongo import mongo_writer


def update_dic(str_line):
    key_str = str_line.split(":")[0]
    value_str = str_line[len(key_str) + 1:]
    print(key_str + ":" + value_str)
    stat_dic.setdefault(key_str, value_str)


# xdrload stat 查询，计算入库效率
if __name__ == '__main__':
    print("-----")
    # stat_line = '{"FileName":"H_GYM-3_20200820151010.00679.sms.A";' \
    #             '"FileSize":"19844";"Earlist":"20200820";"latest":"20200820;"}'.replace(";", ",")
    stat_line = 'FileName:H_GYM-3_20200820151010.00679.sms.A;FileSize:19844;' \
                'Earlist:20200820;latest:20200820;ExtALines:0;TotalLines:17;' \
                'CorrectLines:17;ErrorLines:0;SendLines:17;NonTimeoutExceptionLines:0;' \
                'TimeoutExceptionLines:0;StartTime:20201216190420;EndTime:20201216190420;' \
                'Speed:61;TopicList:{CloudXdrTopic0.2:8;CloudXdrTopic1.2:4;CloudXdrTopic2.2:5;};' \
                'BillDateList:{20200820:17;};TargetLines:0'
    stat_line_new = '{'
    stat_line_list = stat_line.split(";")
    stat_line_list_new = []
    combine_flag = True
    tmp = ''
    stat_dic = {}
    for line in stat_line_list:
        # print('line is :'+line)
        if not line.__contains__('{') and not line.__contains__('}') and combine_flag:
            print('line is :' + line)
            stat_line_list_new.append(line)
            stat_line_new += line
            # key = line.split(":")[0]
            # value = line[len(key) + 1:]
            # stat_dic.setdefault(key, value)
            update_dic(line)

        else:
            combine_flag = False
            tmp += line
            tmp += ','
            #print('tmp is :' + tmp)
            if tmp.__contains__("}"):
                tmp = tmp[:-1]
                print('in if tmp is :' + tmp)
                stat_line_list_new.append(tmp)
                stat_line_new += tmp
                # key = tmp.split(":")[0]
                # value = tmp[len(key)+1:]
                # print(key+":"+value)
                update_dic(tmp)
                combine_flag = True
                tmp = ''

    stat_line_new += '}'
    print("stat_line_new is :" + stat_line_new)
    print("stat_line_list_new")
    print(stat_line_list_new)

    print("stat_dic is :")
    print(stat_dic)

    username = 'root'
    password = 'root'
    mongos_host = '10.19.85.33'
    mongos_port = 34000
    db_name = 'test'
    coll_name = 'stat_emit_20210127'
    writer = mongo_writer
    client = writer.auth(username, password, mongos_host, mongos_port)
    print("-----")
    writer.conn_insertone(client, db_name, coll_name, stat_dic)
