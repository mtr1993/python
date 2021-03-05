#coding=utf-8

import os
import time
import psycopg2
import json
import datetime
import sys

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
from email.mime.base import MIMEBase
from email import encoders
global logger
global roboter

class Conf:
    hosts = roboter.domainParam('bhc_hosts')
    user = roboter.domainParam('bhc_user')
    port = int(roboter.domainParam('bhc_port'))
    password = roboter.decodeParam('bhc_passwd')
    database = roboter.domainParam('bhc_database')

def db_connect():
    try:
        conn = psycopg2.connect(host=Conf.hosts, port=Conf.port, database=Conf.database, user=Conf.user,password=Conf.password,client_encoding="UTF-8")
    except Exception as ex:
        logger.info(ex)
    finally:
        return conn

def send_mail(connect,sms,sts):
    #############
    #要发给谁，这里发给n个人
    flag = False
    cursor = connect.cursor()

    mail_sql = "select mailto_list,mailcc_list from mail_userlist where sts = %s" % sts
    cursor.execute(mail_sql)
    results = cursor.fetchall()
    if len(results) >0:
        for result in results:
            mailto_lists = result[0]
            mailcc_lists = result[1]
            # mailto_lists = 'chenwei3@asiainfo.com'
            # mailcc_lists = 'chenwei3@asiainfo.com'
        mailto_list=mailto_lists.split(",")
        mailcc_list=mailcc_lists.split(",")

        #####################
        #设置服务器，用户名、口令以及邮箱的后缀
        mail_host="mail.asiainfo.com"
        mail_user=u'aihc扫描系统'
        mail_addr="aitech-cmc-aihc@asiainfo.com"
        mail_pass="longer%3"
    #    mail_from=mail_user+"<"+mail_addr+">"
        mail_sender="aitech-cmc-aihc@asiainfo.com"
        mail_from=mail_user+"<"+mail_sender+">"
        ######################

        cur_date=time.strftime("%W")


        msg = MIMEMultipart()
        msg['Subject'] = u'研发团队主机使用情况告警'
        msg['From'] = mail_from
        msg['To'] = ";".join(mailto_list)
        msg['Cc'] = ";".join(mailcc_list)
        body = '<font style="font-weight:bold;  color: #055584;font-size: 1.75em;font-weight: bold;margin: 0 0 .5em;text-shadow:1px 1px 1px #c0e0f2;">您团队主机资源告警如下：</font><br><br>'
        body = body + '<table width="50%" align="left"><tr>'
        for text in sms.split(','):
            alarm_ip = text[4:text.find('主机')]
            alarm_time = text[-19:]
            alarm_info = ''
            alarm_detal = ''
            body = body + '<tr><table align="left" border="1"  bordercolor="black" cellspacing="0px" cellpadding="4px" style="border-collapse:collapse;margin-bottom:30px">'
            body = body + '<tr><td bgcolor="#055584"><font color="#c0e0f2">告警主机</font></td><td bgcolor="#d2ebf9" width="600">'
            body = body + alarm_ip
            body = body + '</td></tr><tr><td bgcolor="#055584"><font color="#c0e0f2">告警时间</font></td><td bgcolor="#d2ebf9" width="600">'
            body = body + alarm_time
            body = body + '</td></tr><tr><td bgcolor="#055584"><font color="#c0e0f2">告警信息</font></td><td bgcolor="#FF3333" width="600">'

            for single_info in text.split('，'):
                if single_info.find('磁盘使用量已达到') == 0:
                    if single_info[-6:] == '达到100%':
                        tmp_info = '100%'
                    else:
                        tmp_info = single_info[-6:]
                    if len(alarm_info) == 0:
                        alarm_info = '磁盘使用率：' + tmp_info
                    else:
                        alarm_info = alarm_info + '；磁盘使用率：' + tmp_info
                    alarm_detal = '磁盘使用量已达到100%，监控系统已无法收发监控信息，请在清理磁盘后执行健康系统用户robot-node/bin/startup.sh脚本重启监控应用。'
                if single_info.find('监控磁盘路径') == 0:
                    alarm_detal = '磁盘路径：' + single_info[single_info.find('/'):] + '。目前支持当监控目录磁盘占用大于90%时监控系统自动调用清理脚本保障磁盘使用空间。清理脚本放置位置在本机健康系统用户默认目录下的robot-node/bin中，脚本名称clean_disk.sh，脚本内容自行添加并保证健康系统用户有足够的删除权限。'
                if single_info.find('CPU') == 0:
                    if len(alarm_info) == 0:
                        alarm_info = 'CPU使用率：' + single_info[single_info.find('达到')+2:]
                    else:
                        alarm_info = alarm_info + '；CPU使用率：' + single_info[single_info.find('达到')+2:]
                if single_info.find('内存') == 0:
                    if len(alarm_info) == 0:
                        alarm_info = '内存使用率：' + single_info[single_info.find('达到')+2:]
                    else:
                        alarm_info = alarm_info + '；内存使用率：' + single_info[single_info.find('达到')+2:]
                if single_info.find('监控已掉线') == 0:
                    if len(alarm_info) == 0:
                        alarm_info = '监控已掉线'
                    else:
                        alarm_info = alarm_info + '监控已掉线'
                    alarm_detal = '请在保证具备java1。8版本且磁盘占用量未满100，执行健康系统用户robot-node/bin/startup.sh脚本重启监控应用'
            body = body + alarm_info
            body = body + '</td></tr><tr><td bgcolor="#055584"><font color="#c0e0f2">问题详情</font></td><td bgcolor="#d2ebf9" width="600">'
            body = body + alarm_detal
            body = body + '</td></tr></table></tr>'
        body = body + '</tr></table>'
        msg.attach(MIMEText(body, 'html', 'utf-8'))

        try:
            #print ms
            s = smtplib.SMTP()
            s.connect(mail_host)
            s.login(mail_addr,mail_pass)
            s.sendmail(mail_from, mailto_list+mailcc_list, msg.as_string())
            s.close()
            flag = True
        except Exception as ex:
            logger.info(ex)
    return flag

if __name__ == '__main__':
    db_connect = db_connect()
    try:
        event_sql = "select a.SEQUENCE_ID,a.ADDITIONAL_TEXT,c.remark from hc_alarm_events a,pm_host_property c where a.OBJECT_ID = c.ADDRESS order by c.remark desc"

        cur = db_connect.cursor()
        cur.execute(event_sql)
        event_results = cur.fetchall()
        evnent_seq = ''
        tmp_sts = ''
        if len(event_results) >0:
            for event_result in event_results:
                if len(evnent_seq) < 2:
                    evnent_seq = str(event_result[0])
                else:
                    evnent_seq = evnent_seq + ',' + str(event_result[0])
                if len(tmp_sts) == 0:
                    tmp_sts = event_result[2]
                    tmp_text = event_result[1]
                elif tmp_sts == event_result[2]:
                    tmp_text = tmp_text + ',' + event_result[1]
                else:
                    is_send = send_mail(db_connect,tmp_text,tmp_sts)
                    tmp_sts = event_result[2]
                    tmp_text = event_result[1]
            is_send = send_mail(db_connect,tmp_text,tmp_sts)

            del_evnent_sql = "DELETE from hc_alarm_events where SEQUENCE_ID in (%s)" % evnent_seq

            if is_send:
                cur.execute(del_evnent_sql)
                db_connect.commit()
    finally:
        db_connect.close()