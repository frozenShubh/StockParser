import MySQLdb
import operator
import urllib2
from sets import Set

# Open database connection
db = MySQLdb.connect("localhost","root","shubham","market")
cursor = db.cursor()

def num(s):
    try:
        return int(s)
    except ValueError:
        return float(s)

cursor.execute("select (b.percentage - a.percentage)/a.percentage as variance, a.id, a.category from holding a , holding b where a.category=b.category and a.id = b.id and a.holdingdate = date(now() - interval 1 day)  and b.holdingdate=date(now())");

data = cursor.fetchall()
for(variance, id, category) in data:
 dec = 'h'
 if(variance==0):
  dec='h'
 if(variance<0):
  dec='s'
 if(variance>0):
  dec='b'
 cursor.execute("insert into variance (id, variance, category, decision, holdingdate) values (%s,%s,%s,%s,date(now()))",(id, variance, category, dec)) 
db.commit();
db.close();
