from pymongo import Connection
import hashlib

class eventClass:
	hour=0,
	value=0,
	def __init__(self):
		self.hour='00'
		self.value='0'
	def __init__(self,hour,value):
		self.hour=int(hour)
		self.value=value
	def __getitem__(self,VID):
		rv='self.'+str(VID)
		return eval(rv)

conn=Connection()
db=conn.weather


def find_events(FQUERY):
	started=False
	DAYSET=db.hourlies.find(FQUERY).sort('hour',1)
	for rec in DAYSET:
		if rec['hour']!='01' and started==False:
			continue
		else:
			started=True
			if rec['hour']=='01':
				init_wdx=eventClass(rec['hour'],rec['wdx'])
				cur_wdx=eventClass(rec['hour'],rec['wdx'])
				init_wspd=eventClass(rec['hour'],rec['wspd'])
				cur_wspd=eventClass(rec['hour'],rec['wspd'])
		
	print init_wdx['hour'],cur_wspd['value'],init_wspd['value']
	return

FQuery={'station':'KMSN','D':2,'M':11,'Y':2013}
find_events(FQuery)


	
