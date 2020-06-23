#import asyncio
#import logging
from uuid import uuid4
#import websockets
import msgpack
import redis

c = redis.from_url("redis://localhost:6379")
idx = str(uuid4())
run = {'method':'add','type':'instant','args':[1,4],'kwargs':{},'id':idx}
c.rpush('syntelly:general',msgpack.packb(run, use_bin_type=True))
print('syntelly:general:{}'.format(idx))
print(c.blpop('syntelly:general:{}'.format(idx))[1])
'''
async def hello():
    uri = "ws://localhost:8000/1"
    async with websockets.connect(uri) as websocket:
        #name = input("What's your name? ")
        run = {'method':'calc','type':'instant','args':['CCC'],'kwargs':{},'id':str(uuid4())}
        #run = {'type':'cancel','id': 'a5da084a-c191-4725-9f8a-ef48515a44bd'}
        await websocket.send(msgpack.packb(run, use_bin_type=True))
        #print(f"> {name}")

        greeting = await websocket.recv()
        print(msgpack.unpackb(greeting))
        greeting = await websocket.recv()
        print(msgpack.unpackb(greeting))
        greeting = await websocket.recv()
        print(msgpack.unpackb(greeting))

'''
#asyncio.get_event_loop().run_until_complete(hello())


'''
class simplyRPClient():
    
    @classmethod
    def rpc(cls, f):
        def registry_method(self, f):
            self.functions[format(f.__name__)] = f
        registry_method(cls,f)
        return f

    def run(self,host,port):

        async def server(websocket, path):
            logging.debug("new client: {} , path:{}".format(websocket,path))
            self.client_id2ws[path] = websocket

            async def consumer_handler(websocket, path):
                async for message in websocket:
                    print(len(message))
                    print(message)
                    head = message[:4]
                    if head == b'zlib':
                        logging.debug('Zlib message')
                        message = zlib.decompress(message[4:])
                    call = msgpack.unpackb(message, raw=False)
                    logging.debug("new message {}".format(message))
                    result = {}
                    try:
                        #call['method'] = "___{}".format(call['method'])
                        #timeout = 3600 if 'timeout' is in call else call['timeout']
                        if call['type'] == 'instant':
                            res = self.functions[call['method']](*call['args'], **call['kwargs'])
                            result.update({'status': 'ok', 'result': res,'id':call['id']})
                        elif call['type'] == 'delayed':
                            future = self.pool.schedule(self.functions[call['method']],call['args'], call['kwargs'],timeout=3600)
                            task = call['id']
                            self.running_tasks[task] = (path, future)
                            result.update({'status': 'running', 'id': task})
                        elif call['type'] == 'cancel':
                            logging.debug("cancelling task {}".format(call['id']))
                            self.running_tasks[call['id']][1].cancel()
                            if self.running_tasks[call['id']][1].cancelled():
                                logging.debug("task {} is cancelled".format(call['id']))
                                result = {'status':'cancelled','id':self.running_tasks[call['id']]}
                                del self.running_tasks[call['id']]
                        #elif call['type'] == 'pending':
                        #    result = {'status': 'ok', 'pending': self.running_tasks}

                    except Exception as e:
                        if 'id' in call:
                            task = call['id']
                        else:
                            task = None
                        result.update(
                            {'status': 'error', 'type': type(e).__name__,'id':task, 'exception': traceback.format_exc()})
                    logging.debug("result {}".format(result))
                    await websocket.send(msgpack.packb(result, use_bin_type=True))

            async def _check_futures():
                while True:
                    to_remove = set()
                    await asyncio.sleep(0.1)
                    for task, (client, future) in self.running_tasks.items():
                        if future.done():
                            if future.exception():
                                result = {'status': 'error','id': str(task), 'type': str(future.exception())}
                            else:
                                result = {'status': 'ok', 'id': str(task), 'result': future.result()}
                        elif future.cancelled():
                            result = {'status': 'cancelled', 'id': str(task)}
                        if future.done() or future.cancelled():
                            try:
                                await self.client_id2ws[client].send(msgpack.packb(result, use_bin_type=True))
                                to_remove.add(task)
                            except:
                                pass

                    for task in to_remove:

                        del self.running_tasks[task]
            consumer_task = asyncio.ensure_future(
                consumer_handler(websocket, path))
            futures_task = asyncio.ensure_future(
                _check_futures())

            await asyncio.wait(
                [consumer_task,futures_task],
                return_when=asyncio.ALL_COMPLETED,
            )

        logging.info("Running on {}:{}".format(host,port))
        start_server = websockets.serve(server, host, port)
        asyncio.get_event_loop().run_until_complete(start_server)
        asyncio.get_event_loop().run_forever()
'''