import copy
import inspect
import logging
import threading
import types
from concurrent.futures import ThreadPoolExecutor
from enum import IntEnum
import redis
from functools import wraps
import msgpack
import zlib
import traceback
from pebble import ProcessPool

logger = logging.getLogger('simply')

class Runtype(IntEnum):
    #Either the request is instant or delayed
    Instant = 0
    Delayed = 1

class TimeoutException(Exception):
    pass

class SimplyRedisServer():
    functions = {} #Dict with functions
    predefined_kwargs = {} #Dict with parameters from the decorator
    timeouts = {}
    running_tasks = {}
    inverse_running_tasks = {}
    pool = ThreadPoolExecutor()

    def __init__(self, path, name, plugin, results_shortlist_timeout=30,results_longterm_timeout=259200):
        self.redis = redis.from_url(path)
        self.name = name
        self.plugin = plugin
        self.results_shortlist_timeout = results_shortlist_timeout
        self.results_longterm_timeout = results_longterm_timeout
        self.running = True
        self._loop = threading.Thread(target=self._run)
        self._loop.start()


    def rpc(self, *args,**kwargs):
        def wrapper(f):
            self.functions[format(f.__name__)] = f
            self.predefined_kwargs[format(f.__name__)] = kwargs
            self.timeouts[format(f.__name__)] = kwargs['timeout'] if 'timeout' in kwargs else -1
            return wrapper
        return wrapper



    def _run(self):

        def _progress_callback(progress=None,message=None,task_id=None):
            result = {"status": 'running', 'progress': progress, 'message': message,'id': task_id}
            self.redis.rpush("{}:general:{}".format(self.name, task_id), msgpack.packb(result, use_bin_type=True))
            self.redis.expire("{}:general:{}".format(self.name, task_id), self.results_shortlist_timeout)

            self.redis.set("{}:state:{}".format(self.name, task_id), msgpack.packb(result, use_bin_type=True),ex=self.results_shortlist_timeout)


        def _done_callback(future,task_id=None):
            try:
                result = {"status": 'ok','result':future.result(),'id':task_id}
            except Exception as e:
                result = {'status': 'error', 'type': type(e).__name__, 'id': task_id, 'exception': traceback.format_exc()}

            self.redis.rpush("{}:general:{}".format(self.name, task_id), msgpack.packb(result, use_bin_type=True))
            self.redis.expire("{}:general:{}".format(self.name, task_id), self.results_shortlist_timeout)

            self.redis.set("{}:state:{}".format(self.name, task_id), msgpack.packb(result, use_bin_type=True),ex=self.results_longterm_timeout)

        queue = "{}:{}".format(self.name, self.plugin)
        processing = "{}:processing:{}".format(self.name, self.plugin)
        while self.running:
            message = self.redis.brpoplpush(queue, processing, 1)
            if not message: continue
            logger.debug("Message: {}".format(message))
            head = message[:4]
            if head == b'zlib':
                logging.debug('Zlib message')
                message = zlib.decompress(message[4:])
            call = msgpack.unpackb(message, raw=False)
            logging.debug("new message {}".format(message))
            result = {}
            fname = call['method']
            try:
                if call['type'] == 'instant':
                    res = self.functions[fname](*call['args'], **call['kwargs'])
                    result.update({'status': 'ok', 'result': res, 'id': call['id']})
                elif call['type'] == 'delayed':
                    task = call['id']

                    progress_callback_with_id = types.FunctionType(_progress_callback.__code__, _progress_callback.__globals__, name=task,argdefs=(None,None,task),
                           closure=_progress_callback.__closure__)

                    done_callback_with_id = types.FunctionType(_done_callback.__code__,_done_callback.__globals__, name=task,argdefs=(None, task),
                                                                   closure=_done_callback.__closure__)
                    call['kwargs'].update({'callback': progress_callback_with_id})

                    future = self.pool.submit(self.functions[fname], *call['args'], **call['kwargs'])
                    future.add_done_callback(done_callback_with_id)
                    self.running_tasks[task] = future
                    self.inverse_running_tasks[future] = task
                    result.update({'status': 'initiated', 'id': task})

                elif call['type'] == 'cancel':
                    logging.debug("cancelling task {}".format(call['id']))
                    self.running_tasks[call['id']][1].cancel()
                    if self.running_tasks[call['id']][1].cancelled():
                        logging.debug("task {} is cancelled".format(call['id']))
                        result = {'status': 'cancelled', 'id': self.running_tasks[call['id']]}
                        self.redis.set("{}:state:{}".format(self.name, call['id']),
                                       msgpack.packb(result, use_bin_type=True), ex=self.results_longterm_timeout)
                        del self.running_tasks[call['id']]

            except Exception as e:
                if 'id' in call:
                    task = call['id']
                else:
                    task = None
                result.update(
                    {'status': 'error', 'type': type(e).__name__, 'id': task, 'exception': traceback.format_exc()})

            self.redis.rpush("{}:general:{}".format(self.name, call['id']), msgpack.packb(result, use_bin_type=True))
            self.redis.expire("{}:general:{}".format(self.name, call['id']), self.results_shortlist_timeout)

    def stop(self):
        self.running = False
        self._loop.join()
