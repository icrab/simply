import redis

from simply.simplyRPClient import SimplyRedisClient

c = SimplyRedisClient('localhost',6379,'mock')
print(c.call('add',[1,2],{}))
#connection = redis.from_url("redis://localhost:6379")
#connection.rpush('syntelly_calls',"Hi")
#connection.mset({"Croatia": "Zagreb", "Bahamas": "Nassau"})
#print(connection.get('Bahamas'))