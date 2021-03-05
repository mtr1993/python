import json
import time
from .transaction import Sender,Receiver

class PatrolSender(Sender):
    def __init__(self, transaction, subscriber):
        super(PatrolSender, self).__init__(transaction, subscriber)

    def set_script(self, script_id, script_name, script_type, timeout):
        self.add_header("task-id", script_id)
        self.add_header("script-type", script_type)
        self.add_header("script-name", script_name)
        self.add_header("timeout", timeout)

    def set_parameter(self, parameter):
        self.add_header("parameter", parameter)

    def set_publisher(self, publisher):
        self.add_header("publisher", publisher)

class PatrolReceiver(object):
    def __init__(self, message):
        self._message = message

    def get_performer(self):
        self._message.get_header_value("performer")

    def get_task_id(self):
        self._message.get_header_value("task-id")

    def get_md5(self):
        self._message.get_header_value("md5sum")

    def get_score(self):
        self._message.get_header_value("score")

    def get_result(self):
        body = self._message.get_body()
        return body

class AlarmEvent(object):
    def __init__(self):
        self._id = None
        self._type = None
        self._object = None
        self._source = None
        self._problem = None
        self._timestamp = None
        self._additional = {}

    def setId(self, id):
        self._id = id

    def setType(self, type):
        self._type = type

    def setObject(self, obj):
        self._object = obj

    def setSource(self, source):
        self._source = source

    def setProblem(self, problem):
        self._problem = problem

    def setAdditional(self, additional):
        self._additional = additional

    def timestamp(self, timestamp):
        self._timestamp = timestamp

    def additional(self, key, value):
        self._additional[key] = value

    def toJSON(self):
        result = {}

        if self._id is not None:
            result['id'] = self._id

        if self._type is not None:
            result['type'] = self._type

        if self._object is not None:
            result['object'] = self._object

        if self._source is not None:
            result['source'] = self._source

        if self._problem is not None:
            result['problem'] = self._problem

        if self._timestamp is not None:
            result['timestamp'] = self._timestamp
        else:
            result['timestamp'] = int(time.time()) * 1000

        result['additional'] = self._additional

        return json.dumps(result, ensure_ascii=False)
