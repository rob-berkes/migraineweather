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
			cur_aread=init_aread
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
		try:
			cur_cheight=eventClass(rec['hour'],int(rec['cheight']))
		except ValueError:
			cur_cheight=eventClass(rec['hour'],0)
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
		if cur_temp['value']-10.0 >= init_temp['value']:
			eventlist.append(hBld_event(FQUERY,init_temp,cur_temp,'temp10ptrise',4))
			init_temp=cur_temp
		if cur_temp['value']+10.0 <= init_temp['value']:
			eventlist.append(hBld_event(FQUERY,init_temp,cur_temp,'temp10ptdrop',4))
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

		if cur_aread['value']+25 <= init_aread['value']:
			eventlist.append(hBld_event(FQUERY,init_aread,cur_aread,'bar25ptdrop',3))
			init_aread=cur_aread
		if cur_aread['value']-25 >= init_aread['value']:
			eventlist.append(hBld_event(FQUERY,init_aread,cur_aread,'bar25ptrise',3))
			init_aread=cur_aread
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
def fnGetSimQueryList(RUNLENGTH,elist,hour_length):
	STATIONLIST=loadStationList()
	ccovdxMatches=0
	wgustsMatches=0
	humRiDrMatches=0
	hum20ptriseMatches=0
	hum25ptdropMatches=0
	temp20ptriseMatches=0
	temp20ptdropMatches=0
	QUERYLIST=[]
	for COUNT in range(0,RUNLENGTH):
    		D=random.randint(2,6)
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
		QUERYLIST.append(SQUERY)
	return QUERYLIST
		
def monteCarlo_user(IDLIST):
	USER='Rob'
	elist=['ccovdx',
		'wgusts',
		'hum20ptrise',
		'hum25ptdrop',
		'temp20ptrise',
		'temp20ptdrop',
		'temp10ptrise',
		'temp10ptdrop',
		'cheight5krise',
		'cheight5kdrop',
		'dewpt50ptrise',
		'dewpt50ptdrop',
		'bar25ptrise',
		'bar25ptdrop',
		'bar50ptrise',
		'bar50ptdrop',
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
	    RUNLENGTH=500
	    SIMLIST=fnGetSimQueryList(RUNLENGTH,elist,hour_length)
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
	ccovdxMatches=0
	wgustsMatches=0
	humRiDrMatches=0
	hum20ptriseMatches=0
	hum25ptdropMatches=0
	temp20ptriseMatches=0
	temp20ptdropMatches=0
	temp10ptriseMatches=0
	temp10ptdropMatches=0
	cheight5kriseM=0
	cheight5kdropM=0
	dewpt50ptriseM=0
	dewpt50ptdropM=0
	bar25ptriseM=0
	bar25ptdropM=0
	bar50ptriseM=0
	bar50ptdropM=0
	for case in SCORELIST:
    		if case['ccovdx']==ecount['ccovdx']:
    			ccovdxMatches+=1
    		if case['wgusts']==ecount['wgusts']:
    			wgustsMatches+=1
		if case['hum20ptrise']>0 and rcount['hum25ptdrop']>0:
			humRiDrMatches+=1
		if case['hum20ptrise']==ecount['hum20ptrise']:
			hum20ptriseMatches+=1
		if case['hum25ptdrop']==ecount['hum25ptdrop']:
			hum25ptdropMatches+=1
		if case['temp20ptrise']==ecount['temp20ptrise']:
			temp20ptriseMatches+=1
		if case['temp20ptdrop']==ecount['temp20ptdrop']:
			temp20ptdropMatches+=1
		if case['temp10ptrise']==ecount['temp10ptrise']:
			temp10ptriseMatches+=1
		if case['temp10ptdrop']==ecount['temp10ptdrop']:
			temp10ptdropMatches+=1
		if case['cheight5krise']==ecount['cheight5krise']:
			cheight5kriseM+=1
		if case['cheight5kdrop']==ecount['cheight5kdrop']:
			cheight5kdropM+=1
		if case['dewpt50ptdrop']==ecount['dewpt50ptdrop']:
			dewpt50ptdropM+=1
		if case['dewpt50ptrise']==ecount['dewpt50ptrise']:
			dewpt50ptriseM+=1
		if case['bar25ptrise']==ecount['bar25ptrise']:
			bar25ptriseM+=1
		if case['bar25ptdrop']==ecount['bar25ptdrop']:
			bar25ptdropM+=1
	print str(RUNLENGTH)+" case histories run. Match report: "
	print "\t\t"+str(ecount['ccovdx'])+"\tccovdx:\t\t"+str(ccovdxMatches)
	print "\t\t"+str(ecount['wgusts'])+"\twgusts:\t\t"+str(wgustsMatches)
	print "\t\t"+str(ecount['hum20ptrise'])+"\thum20ptrise:\t"+str(hum20ptriseMatches)
	print "\t\t"+str(ecount['hum25ptdrop'])+"\thum25ptdrop:\t"+str(hum25ptdropMatches)
	print "\t\t"+str(ecount['temp20ptrise'])+"\ttemp20ptrise:\t"+str(temp20ptriseMatches)
	print "\t\t"+str(ecount['temp20ptdrop'])+"\ttemp20ptdrop:\t"+str(temp20ptdropMatches)
	print "\t\t"+str(ecount['temp10ptrise'])+"\ttemp10ptrise:\t"+str(temp10ptriseMatches)
	print "\t\t"+str(ecount['temp10ptdrop'])+"\ttemp10ptdrop:\t"+str(temp10ptdropMatches)
	print "\t\t"+str(ecount['cheight5krise'])+"\tcheight5krise:\t"+str(cheight5kriseM)
	print "\t\t"+str(ecount['cheight5kdrop'])+"\tcheight5kdrop:\t"+str(cheight5kdropM)
	print "\t\t"+str(ecount['dewpt50ptdrop'])+"\tdewpt50ptdrop:\t"+str(dewpt50ptdropM)
	print "\t\t"+str(ecount['dewpt50ptrise'])+"\tdewpt50ptrise:\t"+str(dewpt50ptriseM)
	print "\t\t"+str(ecount['bar25ptrise'])+"\tbar25ptrise:\t"+str(bar25ptriseM)
	print "\t\t"+str(ecount['bar25ptdrop'])+"\tbar25ptddrop:\t"+str(bar25ptdropM)
	print "==================================="	
        return
def main_runMonteCarlo():
    IDLIST=[]
    IDRS=db.mapEvents.find()
    for rec in IDRS:
				
		
	return
def monteCarlo_event(RIDS):
	elist=['ccovdx','wgusts','hum20ptrise','hum25ptdrop','temp20ptrise','temp20ptdrop',]
	for rid in RIDS:
	    ecount={'ccovdx':0,'hum20ptrise':0,'hum25ptdrop':0,'temp20ptrise':0,'temp20ptdrop':0,'wgusts':0}
	    qrec={"recid" : rid}
	    RSET=db.mapEvents.find(qrec)
	    ecount=countEvents(ecount,elist,RSET)
	    mrec=db.migraines.find_one({'_id':rid})
	    hour_length=mrec['end_hour']-mrec['start_hour']
	    print "Migraine event #"+str(rid)+" . ECounts: "
	    print ecount
	    STATIONLIST=loadStationList()
	    RUNLENGTH=100
	    ccovdxMatches=0
	    wgustsMatches=0
	    humRiDrMatches=0
	    hum20ptriseMatches=0
	    hum25ptdropMatches=0
	    temp20ptriseMatches=0
	    temp20ptdropMatches=0
	    for COUNT in range(0,RUNLENGTH):
    		rcount={'ccovdx':0,'hum20ptrise':0,'hum25ptdrop':0,'temp20ptrise':0,'temp20ptdrop':0,'wgusts':0}
    		D=random.randint(2,6)
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
    		rcount=countEvents(rcount,['ccovdx','wgusts','hum20ptrise','hum25ptdrop','temp20ptrise','temp20ptdrop'],RS)
    		if rcount['ccovdx']==ecount['ccovdx']:
    			ccovdxMatches+=1
    		if rcount['wgusts']==ecount['wgusts']:
    			wgustsMatches+=1
		if rcount['hum20ptrise']>0 and rcount['hum25ptdrop']>0:
			humRiDrMatches+=1
		if rcount['hum20ptrise']==ecount['hum20ptrise']:
			hum20ptriseMatches+=1
		if rcount['hum25ptdrop']==ecount['hum25ptdrop']:
			hum25ptdropMatches+=1
		if rcount['temp20ptrise']==ecount['temp20ptrise']:
			temp20ptriseMatches+=1
		if rcount['temp20ptdrop']==ecount['temp20ptdrop']:
			temp20ptdropMatches+=1
	    print str(RUNLENGTH)+" runs done. Match report: "
	    print "		ccovdx: "+str(ccovdxMatches)
	    print "		wgusts: "+str(wgustsMatches)
	    print " 		hum20ptrise: "+str(hum20ptriseMatches)
	    print "		hum25ptdrop: "+str(hum25ptdropMatches)
	    print "		temp20ptrise: "+str(temp20ptriseMatches)
	    print "		temp20ptdrop: "+str(temp20ptdropMatches)
	    print "		hum20ridr: "+str(humRiDrMatches)
	    print "==================================="	
            if ccovdxMatches >= 1 and wgustsMatches >=1 :
                    print "no remarkable patterns found"
            else:
                    print "interesting possibility found!"
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
    	FQUERY={'station':station[0],'D':2,'M':11,'Y':2013}
    	eventlist=find_events(FQUERY)
    	insert_events(eventlist)
    return

main_runMonteCarlo()
#main_bldMigEventsMap()
#main_bldDayEventList()

