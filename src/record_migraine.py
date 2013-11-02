from pymongo import Connection

conn=Connection()
db=conn.weather
start_hour=2
end_hour=17
day=1
month=1
year=2013

db.migraines.insert({'user':'Rob','D':day,'M':month,'Y':year,'start_hour':start_hour,'end_hour':end_hour})
