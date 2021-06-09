from simply import SimplyRedisClient
from simply.simplyRPCServer import SimplyRedisServer, Runtype


def test_instant():
    server = SimplyRedisServer('redis://localhost', 'test', 'mock')
    @server.rpc
    def add(x, y):
        return x + y
    c = SimplyRedisClient("redis://localhost:6379",'test','mock')
    assert c.call('add',[1,2],{}) == 3
    server.stop()

def test_delayed(): #FixMe: tests does not work properly!
    server = SimplyRedisServer('redis://localhost', 'test', 'mock')

    @server.rpc(type=Runtype.Delayed,timeout=10)
    def countdown(x,callback):
        for i in range(x):
            callback(progress=(i,x),message="running {}".format(i))
        return "A missile is launched!"

    c = SimplyRedisClient("redis://localhost:6379", 'test', 'mock')
    assert c.call('countdown',[10],{},type='delayed') == "A missile is launched!"