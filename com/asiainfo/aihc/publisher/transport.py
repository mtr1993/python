import json as JSON
from .transaction import Subscriber,TransactionListener,Transaction,Message,Receiver
from .message import PatrolSender
from typing import List

class ScriptExecutor(object):
    def __init__(self, publisher):
        self._pub = publisher
        self._tx = Transaction()
        self._timeout = 5
        self._sender = None
        self._called = 0
        self._fetched = 0

    def fetch(self):
        receiver = Receiver(self._tx)

        if receiver.fetch(self._timeout):
            self._fetched = self._fetched + 1
            return receiver.get_message()
        else:
            return None


    def fetchall(self):
        message = []

        receiver = Receiver(self._tx)
        while(receiver.fetch(self._timeout)):
            message.append(receiver.get_message())
            self._fetched = self._fetched + 1

            if self._called == self._fetched:
                return message

        return message


    def begin(self):
        self._tx.begin()

        subscriber = Subscriber("patrol")
        self._pub.add_subsciber(subscriber)
        self._pub.add_listener("transaction", TransactionListener())

        self._sender = PatrolSender(self._tx, subscriber)
        self._sender.set_publisher(self._pub.getId())

        self._called = 0
        self._fetched = 0

        return self

    def change_destination(self, destination, ip_addr=False):
        if ip_addr == True:
            destination = str(self.decode(destination))

        self._sender.set_destination(destination)

        return self

    def call(self):
        try:
            sender = self._sender
            self._pub.send(sender.get_destination(), sender.get_headers(), sender.get_body())

            self._called = self._called + 1

            return True
        except Exception as e:
            return False

    def parameter(self, parameter, is_json=True):
        if is_json == True:
            self._sender.set_parameter(parameter)
        else:
            self._sender.set_parameter(JSON.dumps(parameter, separators=(',', ':')))

    def script(self, script_id, script_name, script_type, timeout=5):
        self._sender.set_script(script_id=script_id, script_name=script_name, script_type=script_type, timeout=timeout * 1000)
        self._timeout = timeout

    def invoke(self, destination, script_id, script_name, script_type, timeout=5, ip_addr=False):
        try:
            if self._pub.has_connected() == False:
                return False

            if ip_addr == True:
                destination = str(self.decode(destination))

            sender = self._sender
            sender.set_destination(destination)
            sender.set_script(script_id=script_id, script_name=script_name, script_type=script_type, timeout=timeout*1000)

            self._pub.send(sender.get_destination(), sender.get_headers(), sender.get_body())

            self._timeout = timeout

            self._called = self._called + 1

            return True
        except Exception as e:
            print(e)
            return False

    def end(self):
        self._tx.commit()
        self._called = 0
        self._fetched = 0

    def decode(self, ip):
        data = ip.split('.')

        return (int(data[0]) << 24) + (int(data[1]) << 16) + (int(data[2]) << 8) + int(data[3])
