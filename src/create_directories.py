#/usr/bin/python
import os
wwwprefix='/var/www/html/'
workprefix='/home/rob/Dropbox/Python/weather/www/'
ifile=open('../etc/station.list','r')
for station in ifile :
	tstr=station.strip().split()
	wwwdir=wwwprefix+tstr[0]
	workdir=workprefix+tstr[0]
	print wwwdir
	if not os.path.exists(wwwdir) :
		os.makedirs(wwwdir)
	if not os.path.exists(workdir) :
		os.makedirs(workdir)
