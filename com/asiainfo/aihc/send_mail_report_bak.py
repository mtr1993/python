# coding=utf-8

import os
import time
import psycopg2

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
from email.mime.base import MIMEBase
from email import encoders

global roboter


class Conf:
    hosts = roboter.domainParam('bhc_hosts')
    user = roboter.domainParam('bhc_user')
    port = int(roboter.domainParam('bhc_port'))
    password = roboter.decodeParam('bhc_passwd')
    database = roboter.domainParam('bhc_database')


def db_connect():
    try:
        conn = psycopg2.connect(host=Conf.hosts, port=Conf.port, database=Conf.database, user=Conf.user,
                                password=Conf.password, client_encoding="UTF-8")
    except Exception as ex:
        logger.info(ex)
    finally:
        return conn


def send_mail(connect, path):
    #############
    # 要发给谁，这里发给n个人
    cursor = connect.cursor()
    sql = "select mailto_list,mailcc_list from mail_userlist where sts = 1"
    cursor.execute(sql)
    results = cursor.fetchall()

    for result in results:
        mailto_lists = result[0]
        mailcc_lists = result[1]

    mailto_list = mailto_lists.split(",")
    mailcc_list = mailcc_lists.split(",")

    print(mailto_list)
    print(mailcc_list)
    #####################
    # 设置服务器，用户名、口令以及邮箱的后缀
    mail_host = "mail.asiainfo.com"
    mail_user = u'aihc扫描系统'
    mail_addr = "aitech-cmc-aihc@asiainfo.com"
    mail_pass = "longer%3"
    #    mail_from=mail_user+"<"+mail_addr+">"
    mail_sender = "aitech-cmc-aihc@asiainfo.com"
    mail_from = mail_user + "<" + mail_sender + ">"
    ######################

    cur_date = time.strftime("%W")

    msg = MIMEMultipart()
    msg['Subject'] = u'研发团队主机使用情况监控报告'
    msg['From'] = mail_from
    msg['To'] = ";".join(mailto_list)
    msg['Cc'] = ";".join(mailcc_list)

    body = '<font style=" font-weight:bold;" size="5">研发团队各部门使用主机情况监控月报(注：数据是基于前一个月的监控数据生成)，供各团队做主机使用情况的分析参考！</font><br>各团队明细总览如下：'
    files = os.listdir(path)
    for file in files:
        if file.endswith(".html"):
            if (file == 'template.html') or (file == 'tegether_body.html'):
                continue
            print(file)
            f = open(os.path.join(path, file), 'r')
            body = body + f.read()
    f.close()
    msg.attach(MIMEText(body, 'html', 'utf-8'))

    try:
        # print msg
        s = smtplib.SMTP()
        s.connect(mail_host)
        s.login(mail_addr, mail_pass)
        s.sendmail(mail_from, mailto_list + mailcc_list, msg.as_string())
        s.close()
        return True
    except Exception as e:
        print(e)
        return False

    # def file_together(path,file_name):


#     files = os.listdir(path)
#     newfilelist = []

#     for file in files:
#         if file.endswith(".html"):

#             if (file == 'template.html') or (file == 'tegether_body.html'):
#                 continue
#             newfilelist.append(file)
#     print (newfilelist)
#     print (33331)
#     newfiles = open('./doc_1_20190807/tegether_body.html','w',encoding='UTF-8')

#     print (33332)

#     for newfile in newfilelist:
#         for html in open(os.path.join(path, newfile),'r'):
#             newfiles.write(html)
#     newfiles.write(u'研发团队各部门使用主机情况监控月报(注：数是基于前一个月的监控数据生成)，供各团队做主机使用情况的分析参考！<br>各团队汇总信息文档下载地址<br>计费需求和运维团队：http://aiqcs.asiainfo.com/download/setup/tmp/AIHC/计费需求和运维团队云主机使用情况.docx<br>计费项目交付团队:http://aiqcs.asiainfo.com/download/setup/tmp/AIHC/计费项目交付团队云主机使用情况.docx<br>计费增值方案开发团队:http://aiqcs.asiainfo.com/download/setup/tmp/AIHC/计费增值方案开发团队云主机使用情况.docx<br>计费解决方案集成团队:http://aiqcs.asiainfo.com/download/setup/tmp/AIHC/计费解决方案集成团队云主机使用情况.docx<br>各团队明细总览如下：')
#     return newfiles

if __name__ == '__main__':
    try:

        db_connect = db_connect()

        path = './doc_1_20190807/'
        file_name = '明细总览.html'
        # newfiles = file_together(path,file_name)

        print(2)
        if send_mail(db_connect, path):
            print('ok')
        else:
            print('false')
    finally:
        db_connect.close()