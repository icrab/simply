import logging
import redis
import msgpack
import zlib
import traceback
from pebble import ProcessPool

class SimplyRedisServer():
    def __init__(self,path,name,plugin):
        self.redis = redis.from_url(path)
        self.name  = name
        self.plugin = plugin

    pool = ProcessPool()
    functions = {}
    client_id2ws = {}
    running_tasks = {}
    inverse_running_tasks = {}

    def _numpy_encode(self, obj):
        return zlib.compress(obj.tobytes())

    @classmethod
    def rpc(cls, f):
        def registry_method(self, f):
            self.functions[format(f.__name__)] = f

        registry_method(cls, f)
        return f

    def run(self):

        def finished(future):
            result = {}
            task = self.inverse_running_tasks[future]
            result.update({'status': 'ok', 'result': future.result(), 'id': task})
            self.redis.rpush("{}:general:{}".format(self.name,task),msgpack.packb(result, use_bin_type=True))
            self.redis.expire("{}:general:{}".format(self.name,task),30)

        queue = "{}:{}".format(self.name,self.plugin)
        processing = "{}:processing:{}".format(self.name,self.plugin)
        while True:
            message = self.redis.brpoplpush(queue,processing,5)
            if not message: continue
            head = message[:4]
            if head == b'zlib':
                logging.debug('Zlib message')
                message = zlib.decompress(message[4:])
            call = msgpack.unpackb(message, raw=False)
            logging.debug("new message {}".format(message))
            result = {}
            try:
                if call['type'] == 'instant':
                    res = self.functions[call['method']](*call['args'], **call['kwargs'])
                    result.update({'status': 'ok', 'result': res, 'id': call['id']})
                elif call['type'] == 'delayed':
                    future = self.pool.schedule(self.functions[call['method']], call['args'], call['kwargs'],
                                                timeout=3600)
                    future.add_done_callback(finished)
                    task = call['id']
                    self.running_tasks[task] = future
                    self.inverse_running_tasks[future] = task

                    result.update({'status': 'running', 'id': task})
                elif call['type'] == 'cancel':
                    logging.debug("cancelling task {}".format(call['id']))
                    self.running_tasks[call['id']][1].cancel()
                    if self.running_tasks[call['id']][1].cancelled():
                        logging.debug("task {} is cancelled".format(call['id']))
                        result = {'status': 'cancelled', 'id': self.running_tasks[call['id']]}
                        del self.running_tasks[call['id']]
            except Exception as e:
                if 'id' in call:
                    task = call['id']
                else:
                    task = None
                result.update(
                    {'status': 'error', 'type': type(e).__name__, 'id': task, 'exception': traceback.format_exc()})

            self.redis.rpush("{}:general:{}".format(self.name,call['id']),msgpack.packb(result, use_bin_type=True))
            self.redis.expire("{}:general:{}".format(self.name,call['id']),30)

