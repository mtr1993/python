import random
import threading
import sys
import base64
from queue import Queue
from com.asiainfo.aihc.stomp import ConnectionListener
from com.asiainfo.aihc.stomp.constants import *

class Message(object):
    def __init__(self, headers = {}, body = ""):
        self._headers = headers
        self._body = body

    def get_headers(self):
        return self._headers

    def get_header_value(self, key):
        return self._headers[key]

    def add_header(self, key, value):
        self._headers[key] = value

    def get_body(self):
        if self._headers.get('content-type') == 'java/base64':
            return self.decode_body()
        else:
            return self._body

    def set_body(self, body):
        self._body = body

    def decode_body(self):
        if (sys.version_info.major == 3):
            return str(base64.b64decode(self._body)[7:], 'utf-8')
        else:
            return base64.b64decode(self._body)[7:]

    def clear(self):
        self._headers.clear()
        self._body = None

    def is_signed(self):
        if (sys.version_info.major == 3):
            return self._headers.__contains__('status') == False or self._headers['status'] != '400'
        else:
            return self._headers.has_key('status') == False or self._headers['status'] != '400'

    def not_signed_id(self):
        return self._headers.get('destination').split('/')[-1]

class Subscriber(object):
    def __init__(self, name, ack=True):
        self._id = name
        self._ack = ack
        self._destination = "/" + name + "/"

    def id(self):
        return self._id

    def ack(self):
        return self._ack

    def destination(self):
        return self._destination

class Transaction(object):
    _tid = random.randint(10000,90000)
    _transactions = {}
    _lock = threading.Lock()

    def __init__(self):
        self._transaction = None

    @staticmethod
    def add(tid, headers, body):
        queue = Transaction._transactions.get(tid)
        if queue is None:
            queue = Queue()
            Transaction._transactions[tid] = queue

        queue.put_nowait(Message(headers, body))

    def get_tid(self):
        return self._transaction

    def begin(self):
        self._transaction = str(self._generate_tid())
        Transaction._transactions[self._transaction] = Queue()

    def commit(self):
        Transaction._transactions.pop(self._transaction)

    def get_queue(self):
        return Transaction._transactions[self._transaction]

    def _generate_tid(self):
        try:
            Transaction._lock.acquire()
            Transaction._tid = Transaction._tid + 1
        finally:
            Transaction._lock.release()

        return Transaction._tid

class TransactionListener(ConnectionListener):
    def on_message(self, headers, body):
        tid = headers.get(HDR_TRANSACTION)

        if tid is not None:
            Transaction.add(tid, headers, body)

class Sender(Message):
    def __init__(self, transaction, subscriber):
        super(Sender, self).__init__()

        self._transaction = transaction
        self._subscriber = subscriber
        self._destination = None

        if transaction is not None:
            self.add_header(HDR_TRANSACTION, transaction.get_tid())

    def set_destination(self, destination):
        self._destination = self._subscriber.destination() + destination

    def get_destination(self):
        return self._destination

class Receiver(object):
    def __init__(self, transaction):
        self._transaction = transaction
        self._message = None

    def get_message(self):
        return self._message

    def get_transaction(self):
        return self._transaction

    def fetch(self, timeout):
        queue = self._transaction.get_queue()

        try:
            self._message = queue.get(block=True, timeout=timeout)
        except Exception:
            return False

        return True
