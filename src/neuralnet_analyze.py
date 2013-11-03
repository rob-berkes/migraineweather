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
class eventRec:
	_id=str(),
	evtlevel=int(),
	station=str(),
	D=int(),
	M=int(),
	Y=int(),
	etype=str(),
	old_val=str(),
	new_val=str(),
	init_hour=int(),
	evt_hour=int(),
	def __init__(self,id,evtlvl,st,D,M,Y,etype,o_val,n_val,i_hour,e_hour):
		self._id=id
		self.evtlevel=int(evtlvl),
		self.station=str(st),
		self.D=D,
		self.M=M,
		self.Y=Y,
		self.etype=etype,
		self.old_val=str(o_val),
		self.new_val=str(n_val),
		self.init_hour=i_hour
		self.evt_hour=e_hour
	def __getitem__(self,vid):
		rv='self.'+str(vid)
		return eval(rv)
	def __setitem__(self,vid,vval):
		print  vid, vval[0]
		self.vid=vval
		return
conn=Connection()
db=conn.weather

def hBld_event(FQUERY,init,curr,etype,elvl):
	erec=eventRec('x',elvl,FQUERY['station'],FQUERY['D'],FQUERY['M'],FQUERY['Y'],etype,init.value,curr.value,init.hour,curr.hour)
	erec._id=hashlib.sha1(str(erec)).hexdigest()

	return erec
def find_events(FQUERY):
	started=False
	DAYSET=db.hourlies.find(FQUERY).sort('hour',1)
	eventlist=[]
	for rec in DAYSET:
		if started==False:
			started=True
			init_wdx=eventClass(rec['hour'],rec['wdx'])
			cur_wdx=eventClass(rec['hour'],rec['wdx'])
			init_wspd=eventClass(rec['hour'],rec['wspd'])
			cur_wspd=eventClass(rec['hour'],rec['wspd'])
			init_ccov=eventClass(rec['hour'],rec['ccov'])
			cur_ccov=eventClass(rec['hour'],rec['ccov'])
			init_hum=eventClass(rec['hour'],int(rec['humidity'].strip('%')))
			cur_hum=eventClass(rec['hour'],int(rec['humidity'].strip('%')))
			init_temp=eventClass(rec['hour'],float(rec['tempF']))
			cur_temp=eventClass(rec['hour'],float(rec['tempF']))
		cur_temp=eventClass(rec['hour'],float(rec['tempF']))
		cur_ccov=eventClass(rec['hour'],rec['ccov'])
		cur_hum=eventClass(rec['hour'],int(rec['humidity'].strip('%')))
		if cur_ccov['value'] != init_ccov['value']:
			eventlist.append(hBld_event(FQUERY,init_ccov,cur_ccov,'ccovdx',5))
			init_ccov=cur_ccov
		if cur_hum['value']+25 <= init_hum['value']:
			eventlist.append(hBld_event(FQUERY,init_hum,cur_hum,'hum25ptdrop',3))
			init_hum=cur_hum
		if cur_hum['value']-20 >= init_hum['value']:
			eventlist.append(hBld_event(FQUERY,init_hum,cur_hum,'hum20ptrise',4))
			init_hum=cur_hum
		if cur_temp['value']+20.0 <= init_temp['value']:
			eventlist.append(hBld_event(FQUERY,init_temp,cur_temp,'temp20ptdrop',3))
			init_temp=cur_temp
		if cur_temp['value']-20.0 >= init_temp['value']:
			eventlist.append(hBld_event(FQUERY,init_temp,cur_temp,'temp20ptrise',3))
			init_temp=cur_temp






		if rec['wgusts'] != 0:
			eventrec=eventRec('x',5,FQUERY['station'],FQUERY['D'],FQUERY['M'],FQUERY['Y'],'wgusts',0,int(rec['wgusts']),0,rec['hour'])
			eid=hashlib.sha1(str(eventrec)).hexdigest()
			eventrec._id=eid
			eventlist.append(eventrec)
			
	return eventlist

def insert_events(eventlist):
	for event in eventlist:
		irec={
			'_id':event._id,
			'evtlevel':event['evtlevel'][0],
			'station':str(event.station[0]),
			'D':(event['D'][0]),
			'M':(event['M'][0]),
			'Y':(event['Y'][0]),
			'etype':str(event.etype[0]),
			'old_val':str(event.old_val[0]),
			'new_val':str(event.new_val[0]),
			'init_hour':int(event.init_hour),
			'evt_hour':int(event.evt_hour)}
		db.events.insert(irec)
	return

stationfile=open('/home/ec2-user/migraineweather/etc/station.list','r')
for station in stationfile:
	station=station.strip().split()
	FQUERY={'station':station[0],'D':2,'M':11,'Y':2013}
	eventlist=find_events(FQUERY)
	insert_events(eventlist)
	
