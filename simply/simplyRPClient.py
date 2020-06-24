import uuid
import msgpack
import redis

class SimplyRedisClient():
    def __init__(self,url,name):
        self.redis = redis.from_url(url)
        self.name = name

    def call(self,function,args,kwargs,type='instant'):
        idx = str(uuid.uuid4())
        run = {'method': function, 'type': type, 'args': args, 'kwargs': kwargs, 'id': idx}
        self.redis.rpush('syntelly:{}'.format(self.name), msgpack.packb(run, use_bin_type=True))
        res = msgpack.unpackb(self.redis.blpop('syntelly:general:{}'.format(idx))[1])
        if type == 'delayed':
            res = msgpack.unpackb(self.redis.blpop('syntelly:general:{}'.format(idx))[1])
        if res[b'status'] == b'error':
            raise Exception(res[b'exception'])
        elif res[b'status'] == b'ok':
            return res[b'result']
        else:
            raise Exception("Unknown error: {}".format(res))
