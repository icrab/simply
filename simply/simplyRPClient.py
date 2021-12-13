import uuid
import msgpack
import redis
from tqdm import tqdm

def prepare_msgpkg_request(function,args,kwargs):
    idx = str(uuid.uuid4())
    run = {'method': function, 'type': 'instant', 'args': args, 'kwargs': kwargs, 'id': idx}
    return msgpack.packb(run)

def prepare_msgpkg_multiple_requests_for_one_function_with_args_only(function,list_of_args):
    array = []
    for request in tqdm(list_of_args,desc="preparing requests"):
        idx = str(uuid.uuid4())
        run = {'method': function, 'type': 'instant', 'args': request, 'kwargs': {}, 'id': idx}
        array.append(run)
    return msgpack.packb(array)


class SimplyRedisClient():
    def __init__(self,url,name,plugin):
        self.redis = redis.from_url(url)
        self.name = name
        self.plugin = plugin

    def call(self,function,args,kwargs,type='instant',callback=None):
        if type == 'instant' and callback: raise Exception("There is no way to use instant request with callbacks!")

        idx = str(uuid.uuid4())
        run = {'method': function, 'type': type, 'args': args, 'kwargs': kwargs, 'id': idx}
        self.redis.rpush('{}:{}'.format(self.name,self.plugin), msgpack.packb(run, use_bin_type=True))
        if type == 'instant':
            #Instant requests
            res = msgpack.unpackb(self.redis.blpop('{}:general:{}'.format(self.name,idx))[1],raw=False)
            if res['status'] == 'error':
                raise Exception(res['exception'])
            elif res['status'] == 'ok':
                return res['result']
            else:
                raise Exception("Unknown error: {}".format(res))

        elif type == 'delayed':
            # Delayed requests
            while True:
                response = msgpack.unpackb(self.redis.blpop('{}:general:{}'.format(self.name, idx))[1], raw=False)
                if response['status'] == 'error':
                    raise Exception(response['exception'])
                elif response['status'] == 'ok':
                    return response['result']
                elif response['status'] == 'initiated':
                    pass
                elif response['status'] == 'running':
                    callback(**{"progress":response['progress'],"message":response['message']})
                else:
                    raise Exception("Unknown error: {}".format(response))

        #if type == 'delayed':
        #    res = msgpack.unpackb(self.redis.blpop('{}:general:{}'.format(self.name,idx))[1],raw=False)

