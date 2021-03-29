#coding=utf-8

import sys
import time
import base64
import uuid

import stomp
from stomp.exception import ConnectFailedException

class Connector(object):
    def __init__(self):
        self._uid = str(uuid.uuid1())
        self._conn = None
        self._is_connected = False
        self._listener = []
        self._subscriber = {}
        self._region = None
        self._domain = None
        self._host_port = None;

    def getId(self):
        return self._uid

    def connect(self, host, port, creator = None, localhost = None, connect_wait = False):
        try:
            self._host = host
            self._port = port

            self._conn = stomp.Connection12([(host, port)], vhost = localhost)
            self._conn.set_listener('connect', ConnectListener(self))
#            self._conn.start()

            self._conn.set_listener('disconnect', DisconnectListener(self._conn))

            header = {'host': host, 'agent': 'PyRobot'}
            if creator is not None:
                header['creator'] = creator

            passcode = ''
            if (sys.version_info.major == 3):
                passcode = str(base64.b64encode(self._uid.encode('utf-8')), 'utf-8')
            else:
                passcode = base64.b64encode(self._uid)

            self._conn.connect(self._uid, passcode, wait = connect_wait, headers=header)
        except ConnectFailedException as e:
            self._is_connected = False
            print(e)
        except Exception as e:
            self._is_connected = False
            print(e)

    def disconnect(self):
        if self._conn is not None:
            try:
                self._conn.disconnect()
                for i in range(50):
                    if self._conn.is_connected():
                        time.sleep(0.1)
                    else:
                        break
            finally:
                if self._conn.is_connected():
                    self._conn.transport.disconnect_socket()
                self._conn = None

    def shutdown(self):
        self._conn.transport.disconnect_socket()

    def send(self, destination, headers, body):
        self._conn.send(destination=destination, headers=headers, body=body)

    def add_listener(self, name, listener):
        if self._listener.count(name) == 0:
            self._conn.set_listener(name, listener)

    def add_subsciber(self, subscriber):
        if self._subscriber.get(subscriber.id()) is None:
            self._conn.subscribe(subscriber.destination() + self._uid, subscriber.id(), subscriber.ack())
            self._subscriber[subscriber.id()] = subscriber

    def conntected(self, boolean = True):
        self._is_connected = boolean

    def has_connected(self):
        return self._is_connected

    def send_alarm_event(self, event):
        if self._conn is not None:
            topic = '/queue/alarm/events'
            self._conn.send(body=event, destination=topic, headers={'route': 'redis', 'topic': topic})

class ConnectListener(stomp.ConnectionListener):
    def __init__(self, connector):
        self._connector = connector

    def on_error(self, headers, message):
        print(headers)
        self._connector.conntected(False)

    def on_connected(self, headers, body):
        self._region = headers.get("region-id")
        self._domain = headers.get("domain-id")
        self._host_port = headers.get("session")

        self._connector.conntected()

    def on_disconnected(self):
        self._connector.conntected(False)

class DisconnectListener(stomp.ConnectionListener):
    def __init__(self, connect):
        self._conn = connect

    def on_error(self, headers, message):
        self._conn.transport.disconnect_socket()

    def on_receipt(self, headers, body):
        self._conn.transport.disconnect_socket()
