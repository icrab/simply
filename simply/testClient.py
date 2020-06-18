import redis

connection = redis.from_url("redis://localhost:6379")
connection.rpush('syntelly_calls',"Hi")
#connection.mset({"Croatia": "Zagreb", "Bahamas": "Nassau"})
#print(connection.get('Bahamas'))