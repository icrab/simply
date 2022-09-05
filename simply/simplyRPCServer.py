import logging
import threading
import time
import types
from concurrent.futures import ThreadPoolExecutor
from threading import Event
from enum import IntEnum
import redis
import msgpack
from platform import node
from uuid import uuid4
import zlib
import traceback


def get_logging_level(level):
    return {'debug': logging.DEBUG, 'info': logging.INFO, 'warning': logging.WARNING, 'error': logging.ERROR, 'critical': logging.CRITICAL}[level.lower()]


class Runtype(IntEnum):
    # Either the request is instant or delayed
    Instant = 0
    Delayed = 1


class TimeoutException(Exception):
    pass


class SimplyRedisServer():
    functions = {}  # Dict with functions
    predefined_kwargs = {}  # Dict with parameters from the decorator
    timeouts = {}
    running_tasks = {}
    tasks_event = {}
    inverse_running_tasks = {}
    pool = ThreadPoolExecutor()

    def __init__(self, host, port, name, plugin, level='warning', results_shortlist_timeout=30, results_longterm_timeout=259200):
        #logger = logging.getLogger('simply_{}_{}'.format(name,plugin))
        self.redis = redis.Redis(
            host=host, port=port, db=0, socket_keepalive=True, health_check_interval=10)
        # logging
        logging.basicConfig(
            format='%(asctime)s:%(levelname)s:%(message)s', level=get_logging_level(level))
        self.unique_worker_name = f"{node()}_{uuid4()}"
        self.logger = logging.getLogger("{}:{}".format(name, plugin))
        self.host = host
        self.port = int(port)
        self.name = name
        self.plugin = plugin
        self.results_shortlist_timeout = results_shortlist_timeout
        self.results_longterm_timeout = results_longterm_timeout
        self.running = True
        self._loop = threading.Thread(target=self._run)
        self._loop.start()

    def rpc(self, *args, **kwargs):
        def wrapper(f):
            self.functions[format(f.__name__)] = f
            self.predefined_kwargs[format(f.__name__)] = kwargs
            self.timeouts[format(
                f.__name__)] = kwargs['timeout'] if 'timeout' in kwargs else -1
            return wrapper
        return wrapper

    def _run(self):

        def _progress_callback(progress=None, message=None, task_id=None):
            result = {"status": 'running', 'progress': progress,
                      'message': message, 'id': task_id}
            self.redis.publish("{}:general:{}".format(
                self.name, task_id), msgpack.packb(result, use_bin_type=True))
            #self.redis.expire("{}:general:{}".format(self.name, task_id), self.results_shortlist_timeout)
            self.redis.set("{}:state:{}".format(self.name, task_id), msgpack.packb(
                result, use_bin_type=True), ex=self.results_longterm_timeout)

        def _done_callback(future, task_id=None):
            try:
                result = {"status": 'ok', 'result': future.result(),
                          'id': task_id}
            except Exception as e:
                result = {'status': 'error', 'type': type(
                    e).__name__, 'id': task_id, 'exception': traceback.format_exc()}

            self.redis.publish("{}:general:{}".format(
                self.name, task_id), msgpack.packb(result, use_bin_type=True))
            #self.redis.expire("{}:general:{}".format(self.name, task_id), self.results_shortlist_timeout)

            self.redis.set("{}:state:{}".format(self.name, task_id), msgpack.packb(
                result, use_bin_type=True), ex=self.results_longterm_timeout)

        queue = "{}:{}".format(self.name, self.plugin)
        processing = "{}:processing:{}".format(self.name, self.plugin)
        self.redis.sadd(f"{self.name}:workers",
                        f"{self.plugin}_{self.unique_worker_name}")
        while self.running:
            try:
                self.redis.ping()
                self.redis.set(
                    f"{self.name}:health:{self.plugin}_{self.unique_worker_name}", "alive", ex=1, nx=True)
                self.logger.debug("ping was successful!")
                message = self.redis.brpoplpush(queue, processing, 1)
            except:
                time.sleep(1)
                self.logger.critical(
                    "Connection to Redis failed on reading, trying to reconnect")
                continue

            if not message:
                continue
            self.logger.debug("Message: {}".format(message))
            head = message[:4]
            if head == b'zlib':
                self.logger.debug('Zlib message')
                message = zlib.decompress(message[4:])
            call = msgpack.unpackb(message, raw=False)
            self.logger.debug("new message {}".format(message))
            result = {}
            fname = call['method']
            try:
                if call['type'] == 'instant':
                    self.logger.info("instant call")
                    res = self.functions[fname](
                        *call['args'], **call['kwargs'])
                    result.update(
                        {'status': 'ok', 'result': res, 'id': call['id']})

                elif call['type'] == 'delayed':
                    task_id = call['id']
                    self.logger.info(f"delayed call, id: {task_id}")
                    # event needs for cancel running tasks
                    try:
                        task_event = Event()
                        self.tasks_event[task_id] = task_event

                        self.redis.set(f"{task_id}_status", "initiated")
                        self.redis.set(f"{task_id}_worker",
                                       self.unique_worker_name)
                        progress_callback_with_id = types.FunctionType(_progress_callback.__code__, _progress_callback.__globals__, name=task_id, argdefs=(None, None, task_id),
                                                                       closure=_progress_callback.__closure__)

                        done_callback_with_id = types.FunctionType(_done_callback.__code__, _done_callback.__globals__, name=task_id, argdefs=(None, task_id),
                                                                   closure=_done_callback.__closure__)

                        call['kwargs'].update(
                            {'callback': progress_callback_with_id, 'event': task_event})
                    except Exception as e:
                        self.logger.info(e)

                    future = self.pool.submit(
                        self.functions[fname], *call['args'], **call['kwargs'])
                    future.add_done_callback(done_callback_with_id)
                    self.running_tasks[task_id] = future
                    self.inverse_running_tasks[future] = task_id
                    result.update({'status': 'initiated', 'id': task_id})

                elif call['type'] == 'cancel':
                    task_id = call['id']
                    task_id = self.running_tasks[task_id]
                    event = self.tasks_event[task_id]
                    self.logger.info("cancelling task {}".format(task_id))
                    self.logger.info(f'running tasks: {self.running_tasks}')
                    self.logger.info(
                        f'current task: {task_id}')
                    was_cancelled = task_id.cancel()
                    self.logger.info(
                        f'was cancelled {was_cancelled}')
                    event.set()

                    if (self.running_tasks[task_id].cancelled() or
                            event.is_set()):
                        self.logger.info(
                            "task {} is cancelled".format(task_id))
                        self.redis.set(f"{task_id}_status", "cancelled")
                        result = {'status': 'cancelled',
                                  'id': self.running_tasks[task_id]}
                        try:
                            self.redis.set("{}:state:{}".format(self.name, task_id),
                                           msgpack.packb(result, use_bin_type=True), ex=self.results_longterm_timeout)
                            del self.running_tasks[task_id]
                        except:
                            self.logger.critical(
                                "Fail during redis connect in 'cancel' operation")

            except Exception as e:
                if 'id' in call:
                    task_id = call['id']
                else:
                    task_id = None
                result.update(
                    {'status': 'error', 'type': type(e).__name__, 'id': task_id, 'exception': traceback.format_exc()})

            try:
                self.redis.publish("{}:general:{}".format(self.name, call['id']),
                                   msgpack.packb(result, use_bin_type=True))
                #self.redis.expire("{}:general:{}".format(self.name, call['id']), self.results_shortlist_timeout)
            except:
                time.sleep(1)
                self.logger.critical(
                    "Connection to Redis failed during writing, trying to reconnect")

    def stop(self):
        self.running = False
        self._loop.join()
