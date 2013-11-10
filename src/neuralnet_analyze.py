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
			try:
				init_wdx=eventClass(rec['hour'],int(rec['wdx']))
			except ValueError:
				init_wdx=eventClass(rec['hour'],0)
			cur_wdx=init_wdx
			init_wspd=eventClass(rec['hour'],int(rec['wspd']))
			cur_wspd=init_wspd
			last_wspd=init_wspd
			init_ccov=eventClass(rec['hour'],rec['ccov'])
			cur_ccov=eventClass(rec['hour'],rec['ccov'])
			init_hum=eventClass(rec['hour'],int(rec['humidity'].strip('%')))
			cur_hum=init_hum
			last_hum=init_hum
			init_temp=eventClass(rec['hour'],float(rec['tempF']))
			cur_temp=init_temp
			last_temp=init_temp
			init_aread=eventClass(rec['hour'],int(rec['aread']))
			cur_aread=init_aread
			last_aread=init_aread
		        init_dewpt=eventClass(rec['hour'],float(rec['dewpt']))
			cur_dewpt=init_dewpt
			try:
				init_cheight=eventClass(rec['hour'],int(rec['cheight']))
			except ValueError:
				init_cheight=eventClass(rec['hour'],0)
			cur_cheight=init_cheight	

		cur_aread=eventClass(rec['hour'],int(rec['aread']))
		cur_temp=eventClass(rec['hour'],float(rec['tempF']))
		cur_ccov=eventClass(rec['hour'],rec['ccov'])
		cur_hum=eventClass(rec['hour'],int(rec['humidity'].strip('%')))
		cur_dewpt=eventClass(rec['hour'],float(rec['dewpt']))
		cur_wspd=eventClass(rec['hour'],int(rec['wspd']))
		try:
			cur_wdx=eventClass(rec['hour'],int(rec['wdx']))
		except ValueError:
			cur_wdx=eventClass(rec['hour'],0)
		try:
			cur_cheight=eventClass(rec['hour'],int(rec['cheight']))
		except ValueError:
			cur_cheight=eventClass(rec['hour'],0)

		if int(cur_wdx['value']/90) != int(init_wdx['value']/90):
			eventlist.append(hBld_event(FQUERY,init_wdx,cur_wdx,'wdxdx',5))
			init_wdx=cur_wdx 
		if cur_ccov['value'] != init_ccov['value']:
			eventlist.append(hBld_event(FQUERY,init_ccov,cur_ccov,'ccovdx',5))
			init_ccov=cur_ccov
		if cur_hum['value']+25 <= init_hum['value']:
			eventlist.append(hBld_event(FQUERY,init_hum,cur_hum,'hum25ptdrop',3))
			init_hum=cur_hum
		if cur_hum['value']-20 >= init_hum['value']:
			eventlist.append(hBld_event(FQUERY,init_hum,cur_hum,'hum20ptrise',4))
			init_hum=cur_hum
		if cur_hum['value']-10 >= last_hum['value']:
			eventlist.append(hBld_event(FQUERY,last_hum,cur_hum,'hum10ptrise',5))
			last_hum=cur_hum
		if cur_hum['value']+10 <= last_hum['value']:
			eventlist.append(hBld_event(FQUERY,last_hum,cur_hum,'hum10ptdrop',5))
			last_hum=cur_hum
		if cur_hum['value']-5 >= last_hum['value']:
			eventlist.append(hBld_event(FQUERY,last_hum,cur_hum,'hum5ptrise',5))
			last_hum=cur_hum
		if cur_hum['value']+5 <= last_hum['value']:
			eventlist.append(hBld_event(FQUERY,last_hum,cur_hum,'hum5ptdrop',5))
			last_hum=cur_hum
		if cur_temp['value']+20.0 <= init_temp['value']:
			eventlist.append(hBld_event(FQUERY,init_temp,cur_temp,'temp20ptdrop',3))
			init_temp=cur_temp
		if cur_temp['value']-20.0 >= init_temp['value']:
			eventlist.append(hBld_event(FQUERY,init_temp,cur_temp,'temp20ptrise',3))
			init_temp=cur_temp
		if cur_temp['value']-10.0 >= init_temp['value']:
			eventlist.append(hBld_event(FQUERY,init_temp,cur_temp,'temp10ptrise',4))
			init_temp=cur_temp
		if cur_temp['value']+10.0 <= init_temp['value']:
			eventlist.append(hBld_event(FQUERY,init_temp,cur_temp,'temp10ptdrop',4))
			init_temp=cur_temp
		if cur_temp['value']-5.0 >= last_temp['value']:
			eventlist.append(hBld_event(FQUERY,last_temp,cur_temp,'temp5ptrise',4))
			last_temp=cur_temp
		if cur_temp['value']+5.0 <= last_temp['value']:
			eventlist.append(hBld_event(FQUERY,last_temp,cur_temp,'temp5ptdrop',4))
			last_temp=cur_temp
		if cur_aread['value']+100 <= init_aread['value']:
			eventlist.append(hBld_event(FQUERY,init_aread,cur_aread,'bar100ptdrop',1))
			init_aread=cur_aread
		if cur_aread['value']-100 >= init_aread['value']:
			eventlist.append(hBld_event(FQUERY,init_aread,cur_aread,'bar100ptrise',1))
			init_aread=cur_aread

		if cur_aread['value']+50 <= last_aread['value']:
			eventlist.append(hBld_event(FQUERY,last_aread,cur_aread,'bar50ptdrop',2))
		if cur_aread['value']-50 >= last_aread['value']:
			eventlist.append(hBld_event(FQUERY,last_aread,cur_aread,'bar50ptrise',2))
		if cur_aread['value']+25 <= last_aread['value']:
			eventlist.append(hBld_event(FQUERY,last_aread,cur_aread,'bar25ptdrop',3))
		if cur_aread['value']-25 >= last_aread['value']:
			eventlist.append(hBld_event(FQUERY,last_aread,cur_aread,'bar25ptrise',3))
		if cur_aread['value']-10 >= last_aread['value']:
			eventlist.append(hBld_event(FQUERY,last_aread,cur_aread,'bar10ptrise',4))
		if cur_aread['value']+10 <= last_aread['value']:
			eventlist.append(hBld_event(FQUERY,last_aread,cur_aread,'bar10ptdrop',4))
		if cur_aread['value']-5 >= last_aread['value']:
			eventlist.append(hBld_event(FQUERY,last_aread,cur_aread,'bar5ptrise',4))
		if cur_aread['value']+5 <= last_aread['value']:
			eventlist.append(hBld_event(FQUERY,last_aread,cur_aread,'bar5ptdrop',4))
		last_aread=cur_aread
		if cur_cheight['value']-50 >= init_cheight['value']:	
			eventlist.append(hBld_event(FQUERY,init_cheight,cur_cheight,'cheight5krise',5))
			init_cheight=cur_cheight
		if cur_cheight['value']+50 <= init_cheight['value']:
			eventlist.append(hBld_event(FQUERY,init_cheight,cur_cheight,'cheight5kdrop',5))
			init_cheight=cur_cheight
		if cur_dewpt['value']-5.0 >= init_dewpt['value']:
			eventlist.append(hBld_event(FQUERY,init_dewpt,cur_dewpt,'dewpt50ptrise',5))
			init_dewpt=cur_dewpt
		if cur_dewpt['value']+5.0 <= init_dewpt['value']:
			eventlist.append(hBld_event(FQUERY,init_dewpt,cur_dewpt,'dewpt50ptdrop',5))
			init_dewpt=cur_dewpt
		if cur_wspd['value']+5 <= last_wspd['value']:
			eventlist.append(hBld_event(FQUERY,last_wspd,cur_wspd,'wspd5drop',4))
			last_wspd=cur_wspd
		if cur_wspd['value']-5 >= last_wspd['value']:
			eventlist.append(hBld_event(FQUERY,last_wspd,cur_wspd,'wspd5rise',4))
			last_wspd=cur_wspd
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
		ECOUNT[rec['etype']]+=1
	return ECOUNT
def fnGetSimQueryList(RUNLENGTH,elist,hour_length,s_hour,e_hour):
	STATIONLIST=loadStationList()
	QUERYLIST=[]
	for COUNT in range(0,RUNLENGTH):
    		D=random.randint(2,10)
    		start_hour=random.randint(s_hour-1,s_hour+1)
    		end_hour=random.randint(e_hour-1,e_hour+1)
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
		QUERYLIST.append(SQUERY)
	return QUERYLIST
		
def monteCarlo_user(IDLIST):
	RUNLENGTH=100
	USER='Rob'
	elist=[
		'bar5ptrise',
		'bar5ptdrop',
		'bar10ptrise',
		'bar10ptdrop',
		'bar25ptrise',
		'bar25ptdrop',
		'bar50ptdrop',
		'bar50ptrise',
		'bar100ptdrop',
		'bar100ptrise',
		'ccovdx',
		'cheight5krise',
		'cheight5kdrop',
		'dewpt50ptrise',
		'dewpt50ptdrop',
		'hum5ptrise',
		'hum5ptdrop',
		'hum10ptrise',
		'hum10ptdrop',
		'hum20ptrise',
		'hum25ptdrop',
		'temp5ptrise',
		'temp5ptdrop',
		'temp10ptrise',
		'temp10ptdrop',
		'temp20ptrise',
		'temp20ptdrop',
		'wdxdx',
		'wgusts',
		'wspd5rise',
		'wspd5drop',
		]
	ecount={}
	for item in elist:
		ecount[item]=0
	SREC={'user':USER}
	ePROFILE=[]
	for ID in IDLIST:
		SQ_eRS={'recid':str(ID)}
		eRS=db.mapEvents.find(SQ_eRS)
		ecount=countEvents(ecount,elist,eRS)
		
	print "Summary Ecount for "+str(len(IDLIST))+" records: "+str(ecount)
	CASELENGTH=len(IDLIST)
	print "Now running Monte Carlo sim against user case record..."
	FULLSIMLIST=[]
	for ID in IDLIST:
	    mrec=db.migraines.find_one({'_id':ID})
	    hour_length=mrec['end_hour']-mrec['start_hour']
	    SIMLIST=fnGetSimQueryList(RUNLENGTH,elist,hour_length,mrec['start_hour'],mrec['end_hour'])
	    FULLSIMLIST.append(SIMLIST)
	print "Now scoring scenarios..."
	SCORELIST=[]
	for b in range(0,RUNLENGTH):
		rcount={}
		for item in elist:
			rcount[item]=0
		for a in range(0,CASELENGTH):
			IDX=(a*RUNLENGTH)+b
			RS=db.events.find(FULLSIMLIST[a][b])
			rcount=countEvents(rcount,elist,RS)
		SCORELIST.append(rcount)
	humRiDrMatches=0
	BarRiseM=0
	BarDropM=0
	BarComboM=0
	s_BarComboM=0
	matches={}
	for item in elist:
		matches[item]=0		
	for case in SCORELIST:
    		if case['ccovdx']==ecount['ccovdx']:
    			matches['ccovdx']+=1
    		if case['wgusts']==ecount['wgusts']:
    			matches['wgusts']+=1
		if case['hum20ptrise']>0 and rcount['hum25ptdrop']>0:
			humRiDrMatches+=1
		caseBarRises=case['bar25ptrise']+case['bar50ptrise']+case['bar100ptrise']+case['bar5ptrise']+case['bar10ptrise']
		s_caseBarRises=case['bar5ptrise']+case['bar10ptrise']
		caseBarDrops=case['bar25ptdrop']+case['bar50ptdrop']+case['bar100ptdrop']+case['bar5ptdrop']+case['bar10ptdrop']
		s_caseBarDrops=case['bar5ptdrop']+case['bar10ptdrop']
		eBarRises=ecount['bar25ptrise']+ecount['bar50ptrise']+ecount['bar100ptrise']+ecount['bar5ptrise']+ecount['bar10ptrise']
		s_eBarRises=ecount['bar5ptrise']+ecount['bar10ptrise']
		eBarDrops=ecount['bar25ptdrop']+ecount['bar50ptdrop']+ecount['bar100ptdrop']+ecount['bar5ptdrop']+ecount['bar10ptdrop']
		s_eBarDrops=ecount['bar5ptdrop']+ecount['bar10ptdrop']
		if caseBarRises == eBarRises:
			BarRiseM+=1
		if caseBarDrops == eBarDrops:
			BarDropM+=1
		if (caseBarRises == eBarRises) and (caseBarDrops == eBarDrops):
			BarComboM+=1
		if (s_caseBarRises == s_eBarRises) and (s_caseBarDrops == s_eBarDrops):
			s_BarComboM+=1
		for item in elist:
			if case[item]==ecount[item]:
				matches[item]+=1
	print str(RUNLENGTH)+" case histories run. Match report: "
	print "\tCase Hits\tName\t\tSim Matches"
	print "\t\t"+str(eBarRises)+"\tTotal Bar Rises\t\t"+str(BarRiseM)
	print "\t\t"+str(eBarDrops)+"\tTotal Bar Drops\t\t"+str(BarDropM)
	print "\t\t"+str(eBarRises)+","+str(eBarDrops)+"\tTotal Bar Matches\t"+str(BarComboM)
	print "\t\t"+str(s_eBarRises)+","+str(s_eBarDrops)+"\tSmall Bar Matches\t"+str(s_BarComboM)
	for item in elist:
		print "\t\t"+str(ecount[item])+"\t"+item+"\t\t"+str(matches[item])
	
	print "==================================="	
        return
def main_runMonteCarlo():
    IDLIST=[]
    IDRS=db.mapEvents.find()
    for rec in IDRS:
				
		
	return
def main_runMonteCarlo():
    IDLIST=[]
    IDRS=db.mapEvents.find()
    for rec in IDRS:
    	if rec['recid'] not in IDLIST:
    		IDLIST.append(rec['recid'])
    print IDLIST
    #monteCarlo_event(IDLIST)
    monteCarlo_user(IDLIST)
    return
def main_bldMigEventsMap():
    bldList_migraineEvent()
    return
def main_bldDayEventList():
    stationfile=open('/home/ec2-user/migraineweather/etc/station.list','r')
    for station in stationfile:
    	station=station.strip().split()
    	for D in range(2,11):
		FQUERY={'station':station[0],'D':D,'M':11,'Y':2013}
    		eventlist=find_events(FQUERY)
    		insert_events(eventlist)
    return

main_runMonteCarlo()
#main_bldMigEventsMap()
#main_bldDayEventList()

