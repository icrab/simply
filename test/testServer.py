import threading
import time

from simply import SimplyRedisClient
from simply.simplyRPCServer import SimplyRedisServer, Runtype


def test_instant():
    server = SimplyRedisServer('localhost',6379, 'test', 'mock')
    @server.rpc(type=Runtype.Instant,timeout=10)
    def add(x, y):
        return x + y

    c = SimplyRedisClient("redis://localhost:6379",'test','mock')
    assert c.call('add',[1,2],{}) == 3
    server.stop()

def test_delayed():

    def start_server():
        server = SimplyRedisServer('localhost',6379, 'test', 'mock')
        @server.rpc(type=Runtype.Delayed, timeout=15)
        def countdown(x,callback):
            for i in range(x):
                time.sleep(0.2)
                callback(progress=(i,x),message="running {}".format(i))
            return "A missile is launched!"

    x = threading.Thread(target=start_server, args=())
    x.start()

    c = SimplyRedisClient("redis://localhost:6379", 'test', 'mock')
    global_messsage_collection = []

    def client_callback(progress, message):
        global_messsage_collection.append(f'progress = {progress}, message = {message}')

    result = c.call('countdown',[10],{},type='delayed',callback=client_callback)
    messages = ("|".join(global_messsage_collection))
    assert messages == "progress = [0, 10], message = running 0|progress = [1, 10], message = running 1|progress = [2, 10], message = running 2|progress = [3, 10], message = running 3|progress = [4, 10], message = running 4|progress = [5, 10], message = running 5|progress = [6, 10], message = running 6|progress = [7, 10], message = running 7|progress = [8, 10], message = running 8|progress = [9, 10], message = running 9"
    assert result == "A missile is launched!"

    #server.stop()