#/usr/bin/python
import csv
import urllib2
import pymongo
import datetime
import zipfile
from zipfile import ZipFile
from pymongo import Connection
connection=Connection('10.115.126.7',27017)
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
		elif line.startswith("Pressure") :
			linesplit=line.strip().split()
			pressure=linesplit[2]
		elif line.startswith("Temperature") : 
			linesplit=line.strip().split()
			temperature=linesplit[1]

	ofile=open("/home/ec2-user/migraineweather/logs/"+station[0]+".log","a")
	oline=time+' '+humidity+' '+dewpoint+' '+pressure+' '+temperature+'\n'
	ofile.write(oline)
	ofile.close()
	dbentry={"station" : station[0], "year" : tyear, "day": tday, "month": tmonth, "time": time,"humidity":humidity,"dewpoint":dewpoint,"baropressure":pressure,"temperature": temperature}
	hourlies.insert(dbentry)
	repcopy.close()

