from pymongo import Connection
import hashlib
import random

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
		self.vid=vval
		return
class evtCount:
	_id=str(),
	ccovdx=0,
	hum20ptrise=0,
	hum25ptdrop=0,
	temp20ptdrop=0,
	temp20ptrise=0,
	bar50ptrise=0,
	bar50ptdrop=0,
	bar100ptdrop=0,
	bar100ptrise=0,
	wgusts=0,
	def __init__(self):
		return
conn=Connection()
db=conn.weather
def hBld_Hash(rec):
	hash_bld_str=str(rec.station)+str(rec.D)+str(rec.M)+str(rec.Y)+str(rec.etype)+str(rec.old_val)+str(rec.new_val)+str(rec.init_hour)+str(rec.evt_hour)
	return hashlib.sha1(hash_bld_str).hexdigest()

def hBld_event(FQUERY,init,curr,etype,elvl):
	erec=eventRec('x',elvl,FQUERY['station'],FQUERY['D'],FQUERY['M'],FQUERY['Y'],etype,init.value,curr.value,init.hour,curr.hour)
	erec._id=hBld_Hash(erec)
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
			init_aread=eventClass(rec['hour'],int(rec['aread']))
			cur_aread=eventClass(rec['hour'],int(rec['aread']))
		cur_aread=eventClass(rec['hour'],int(rec['aread']))
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
		if cur_aread['value']+100 <= init_aread['value']:
			eventlist.append(hBld_event(FQUERY,init_aread,cur_aread,'bar100ptdrop',1))
			init_aread=cur_aread
		if cur_aread['value']-100 >= init_aread['value']:
			eventlist.append(hBld_event(FQUERY,init_aread,cur_aread,'bar100ptrise',1))
			init_aread=cur_aread

		if cur_aread['value']+50 <= init_aread['value']:
			eventlist.append(hBld_event(FQUERY,init_aread,cur_aread,'bar50ptdrop',2))
			init_aread=cur_aread
		if cur_aread['value']-50 >= init_aread['value']:
			eventlist.append(hBld_event(FQUERY,init_aread,cur_aread,'bar50ptrise',2))
			init_aread=cur_aread



		if rec['wgusts'] != 0:
			eventrec=eventRec('x',5,FQUERY['station'],FQUERY['D'],FQUERY['M'],FQUERY['Y'],'wgusts',0,int(rec['wgusts']),0,rec['hour'])
			eid=hBld_Hash(eventrec)
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
def bldList_migraineEvent():
	RSET=db.migraines.find()
	recnum=0
	for rec in RSET:
		recnum+=1
		recstr="%05d" % (recnum,)
		recid=str(recstr)+str(rec['user'])
		eventCount=evtCount()
		D=rec['start_day']
		hour=rec['start_hour']
		FQUERY={'station':rec['station'],
			'Y':rec['start_year'],
			'M':rec['start_month'],
			'D':rec['start_day']
			}
		ESET=db.events.find(FQUERY)
		eventlist=[]
		for event in ESET:
			estr="eventCount.'"+str(event['etype'])+"'"
			if event['evt_hour']>=rec['start_hour'] and event['evt_hour']<=rec['end_hour']:
				nrec={'hour':event['evt_hour'],'etype':event['etype'],'recid':rec['_id']}
				eventlist.append(nrec)

		for e in eventlist:
			eid=str(e['hour'])+str(e['etype'])+str(e['recid'])
			eid=hashlib.sha1(eid).hexdigest()
			irec={'_id':str(eid),'hour':e['hour'],'etype':e['etype'],'recid':e['recid']}
			db.mapEvents.insert(irec)
			print e['hour'],e['etype'],e['recid']
		
	return

def loadStationList():
	stationfile=open('/home/ec2-user/migraineweather/etc/station.list','r')
	nlist=[]
	for station in stationfile:
		station=station.strip().split()
		nlist.append(station[0])
	return nlist

def getRandomFromList(SLIST):
	sidx=random.randint(0,len(SLIST)-1)
	return SLIST[sidx]
def countEvents(ECOUNT,ETYPELIST,RECORDSET):
	for rec in RECORDSET:
		if rec['etype'] in ETYPELIST:
			ECOUNT[rec['etype']]+=1
	return ECOUNT
def monteCarlo_event(RIDS):
	ecount={'ccovdx':0,'hum20ptrise':0,'hum25ptdrop':0,'temp20ptrise':0,'temp20ptdrop':0,'wgusts':0}
	for rid in RIDS:
	    qrec={"recid" : rid}
	    RSET=db.mapEvents.find(qrec)
	    ecount=countEvents(ecount,['ccovdx','wgusts',],RSET)
	    mrec=db.migraines.find_one({'_id':rid})
	    hour_length=mrec['end_hour']-mrec['start_hour']
	    print 'Migraine event has '+str(ecount['ccovdx'])+' ccovdx and '+str(ecount['wgusts'])+' wgusts over '+str(hour_length)+' hours. Running monte carlo simulation...'
	    STATIONLIST=loadStationList()
	    ccovdxMatches=0
	    wgustsMatches=0
	    RUNLENGTH=100
	    for COUNT in range(0,RUNLENGTH):
    		rcount={'ccovdx':0,'hum20ptrise':0,'hum25ptdrop':0,'temp20ptrise':0,'temp20ptdrop':0,'wgusts':0}
    		D=random.randint(2,4)
    		start_hour=random.randint(0,9)
    		end_hour=start_hour+hour_length
    		station=getRandomFromList(STATIONLIST)
    		SQUERY={'station':station,
    			'D':D,
    			'M':11,
    			'Y':2013,
    			'evt_hour':
    			{
    				'$gte':start_hour,
    				'$lte':end_hour  },
    			}
    		RS=db.events.find(SQUERY)
    		rcount=countEvents(rcount,['ccovdx','wgusts',],RS)
    		if rcount['ccovdx']==ecount['ccovdx']:
    			ccovdxMatches+=1
    		if rcount['wgusts']==ecount['wgusts']:
    			wgustsMatches+=1
	
    	    print "done.  After "+str(RUNLENGTH)+" runs, "+str(ccovdxMatches)+" ccovdx Matches and "+str(wgustsMatches)+" wgusts Matches found."
            if ccovdxMatches >= 1 and wgustsMatches >=1 :
                    print "no remarkable patterns found"
            else:
                    print "interesting possibility found!"
        return
#stationfile=open('/home/ec2-user/migraineweather/etc/station.quit()list','r')
#for station in stationfile:
#	station=station.strip().split()
#	FQUERY={'station':station[0],'D':4,'M':11,'Y':2013}
#	eventlist=find_events(FQUERY)
#	insert_events(eventlist)
#bldList_migraineEvent()
rids=["1e0b22a31311957459f49fa973995d0f22604cc4","5c5b82ddbdb755892cc18a2b7df2615e427df908",]
monteCarlo_event(rids)
