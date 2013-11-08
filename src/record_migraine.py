from pymongo import Connection
import hashlib


conn=Connection()
db=conn.weather
start_hour=12
end_hour=22
start_day=6
end_day=6
start_month=11
end_month=start_month
start_year=2013
end_year=start_year
station='KMSN'

hId=hashlib.sha1(str('Rob')
		+str(start_month)
		+str(start_year)
		+str(start_day)
		+str(start_hour)
		+str(end_month)
		+str(end_year)
		+str(end_day)
		+str(end_hour)
		+str(station)).hexdigest()

db.migraines.insert({'_id':hId,
		     'user':'Rob',
		     'start_month':start_month,
		     'start_year':start_year,
		     'start_day':start_day,
		     'start_hour':start_hour,
		     'end_month':end_month,
		     'end_year':end_year,
		     'end_day':end_day,
		     'end_hour':end_hour,
		     'station':station})
