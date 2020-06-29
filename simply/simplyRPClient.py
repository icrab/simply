import uuid
import msgpack
import redis

class SimplyRedisClient():
    def __init__(self,url,name,plugin):
        self.redis = redis.from_url(url)
        self.name = name
        self.plugin = plugin

    def call(self,function,args,kwargs,type='instant'):
        idx = str(uuid.uuid4())
        run = {'method': function, 'type': type, 'args': args, 'kwargs': kwargs, 'id': idx}
        self.redis.rpush('{}:{}'.format(self.name,self.plugin), msgpack.packb(run, use_bin_type=True))
        res = msgpack.unpackb(self.redis.blpop('{}:general:{}'.format(self.name,idx))[1],raw=False)
        #print('first ',res)
        if type == 'delayed':
        #    print(res)
            res = msgpack.unpackb(self.redis.blpop('{}:general:{}'.format(self.name,idx))[1],raw=False)
        if res['status'] == 'error':
            raise Exception(res['exception'])
        elif res['status'] == 'ok':
            return res['result']
        else:
            raise Exception("Unknown error: {}".format(res))
