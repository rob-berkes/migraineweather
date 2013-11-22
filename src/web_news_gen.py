from pymongo import Connection

conn=Connection()
db=conn.weather

OFILE=open("news.htm","w")

OFILE.write("<!DOCTYPE html> \
<html class='no-js'> \
    <head> \
        <meta charset='utf-8'> \
        <meta http-equiv='X-UA-Compatible' content='IE=edge'> \
        <title></title> \
        <meta name='description' content=''> \
        <meta name='viewport' content='width=device-width, initial-scale=1'> \
        <!-- Place favicon.ico and apple-touch-icon(s) in the root directory --> \
        <link rel='stylesheet' href='css/normalize.css'> \
        <link rel='stylesheet' href='css/main.css'> \
        <script src='js/vendor/modernizr-2.7.0.min.js'></script> \
    </head> \
    <body bgcolor='#249090'>")

RS=db.news.find()
for rec in RS:
	OFILE.write("<h2><it>"+str(rec['_id'])+"</it></h2>")
	OFILE.write("<h2>"+str(rec['title'])+"</h2>")
	OFILE.write("<h5>"+str(rec['text'])+"</h5><br>")
	OFILE.write("<br>")

OFILE.write("</body></head></html>")

