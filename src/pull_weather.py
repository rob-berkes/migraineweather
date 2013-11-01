#/usr/bin/python
import csv
import urllib2
import pymongo
import datetime
import zipfile
import re
from zipfile import ZipFile
from pymongo import Connection
class wxSet:
	visibility=0,
	wdx=0,
	wspd=0,
	wgusts=0,
	ccov=0,
	cheight=0,
	ctype='',
	atype=0,
	aread=0,
	def __init__(self):
		self.visibility=0
		self.wdx=0
		self.wspd=0
		self.wgusts=0
		self.ccov=0
		self.cheight=0
		self.ctype=0
		self.atype=0
		self.aread=0
	
def decode_metar(sline):
	rset=wxSet()
	re1=re.compile("[QA]\d{4}")
	for word in sline:
		if word.endswith('SM'):
			rset.visibility=word[0:-2]
		if word.endswith('KT'):
			rset.wdx=word[0:3]
			rset.wspd=word[3:5]
			ct=0
			for letter in word:
				if letter=='G':
					rset.wgusts=word[ct+1:ct+3]
				ct+=1
		if word.startswith(('SKC','OVC','CLR','FEW','SCT','BKN','VV')):
			rset.ccov=word[0:3]
			rset.cheight=word[3:6]
			try:
				rset.ctype=word[6:8]
			except:
				rset.ctype=''
		if re1.match(word):
			if word[1]=='Q':
				rset.atype='mbQ'
			elif word[1]=='A':
				rset.atype='hgA'
			rset.aread=word[1:5]

			
	return rset
				
#connection=Connection('10.37.11.218',27017)
connection=Connection()
db=connection.weather
hourlies=db.hourlies
infile=open("/home/ec2-user/migraineweather/etc/station.list","r")
tyear=datetime.date.today().year
tmonth=datetime.date.today().month
tday=datetime.date.today().day
thour=datetime.datetime.now().hour
for station in infile:
	station=station.strip().split()
	newurl="http://weather.noaa.gov/pub/data/observations/metar/decoded/"+station[0]+".TXT"
	repcopy=open("/home/ec2-user/migraineweather/archives/"+station[0]+".txt","a")
#	print newurl
	arcfname=station[0]+str(tyear)+str(tmonth)+str(tday)+str(thour)
	response=urllib2.urlopen(newurl)
	linecount=0
	for line in response:
#		print line
		linecount+=1
		repcopy.write(line)
		if line.startswith("Relative") :
			linesplit=line.strip().split()
			humidity=linesplit[2]
		elif linecount == 2 :
			linesplit=line.strip().split()
			time=linesplit[4]+linesplit[5]
		elif line.startswith("Dew") :
			linesplit=line.strip().split()
			dewpoint=linesplit[2]
		elif line.startswith("Temperature") : 
			linesplit=line.strip().split()
			temperature=linesplit[1]
		elif line.startswith("ob:"):
			linesplit=line.strip().split()
			recset=decode_metar(linesplit)

	dbentry={"station" : station[0], "Y" : tyear, "D": tday, "M": tmonth, "time": time,"humidity":humidity,"dewpt":dewpoint,"tempF": temperature,"wspd":recset.wspd,"vis":recset.visibility,"wdx":recset.wdx,"wgusts":recset.wgusts, 
		 "ccov":recset.ccov,"cheight":recset.cheight,"ctype":recset.ctype,"atype":recset.atype,"aread":recset.aread}
	hourlies.insert(dbentry)
	repcopy.close()

