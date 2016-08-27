import MySQLdb
import math
import operator
import urllib2
from sets import Set
from bs4 import BeautifulSoup
# Open database connection
db = MySQLdb.connect("localhost","root","shubham","market")
cursor = db.cursor()


myPort = dict()
stockmap = dict()
currentPort = Set()
#get top 5 in each category
cursor.execute("select distinct category from variance");
categories = cursor.fetchall()
print categories
for (category) in categories: 
 cursor.execute("select s.id, variance,stockname  from variance v join stocks s on s.id = v.id where decision='b' and category = %s and holdingdate=date(now()) order by variance desc limit 5",(category))
 top5 = cursor.fetchall()
 thisCategoryMap = dict()
 for (id, variance,name) in top5:
  print id, variance, name
  thisCategoryMap[id] = variance
  stockmap[id] = name
 myPort[category] = thisCategoryMap

print myPort 


freeMoney = 0;
#get existing portfolio and sell
cursor.execute("select p.stockid, p.units, p.avgbuyprice, s.stockname, s.url  from portfolio p join stocks s on p.stockid=s.id where userid = %s and holdingdate=date(now() - interval 1 day)", ("shubham"))
portfolio = cursor.fetchall()
for (stock, units, buyprice, stockname, url) in portfolio:
 print "Portfolio stock: " + stock
 cursor.execute("select variance,decision from variance where holdingdate = date(now()) and id=%s",(stock))
 currentPort.add(stock)
 (variance, decs) = cursor.fetchone()
 myPort[stock] = variance
 try:
    responseMain = urllib2.urlopen('http://www.moneycontrol.com/' + url)
    htmlMain = responseMain.read()
    soupMain = BeautifulSoup(htmlMain, 'html.parser')
    bseTicker =  soupMain.find(id='Bse_Prc_tick')
    if(bseTicker!=None):
     if(decs=='s'):
      currentPort.remove(stock)
      print "Sell " + stockname + " priced at : " + bseTicker.contents[0].getText() + " bought at : " + str(buyprice) 
      freeMoney = freeMoney + float(bseTicker.contents[0].getText())*float(units)
     else:
      cursor.execute("insert into portfolio (userid, stockid, holdingdate,units, avgbuyprice, currentprice, targetprice)  values ('shubham',%s,date(now()),%s,%s,%s,%s)", (stock,units,buyprice,float(bseTicker.contents[0].getText()),0))
 except Exception as e:
    print e
    continue

print "total Sell: " + str(freeMoney)

moneyInHand = freeMoney
freeLen = 20 - len(currentPort)
if(freeMoney==0):
 moneyInHand = freeLen*5000
stockLimit = moneyInHand/freeLen
print "Stock limit " + str(stockLimit)
print str(freeLen)
topStock = []
topStockGot = False;
i=freeLen
for (x,y) in sorted(myPort.items(), key=operator.itemgetter(1), reverse=True):
 if(i>=0):
  i=i-1
  try:
    responseMain = urllib2.urlopen('http://www.moneycontrol.com/india/stockpricequote/computers-software/bluestarinfotech/' + x) 
    htmlMain = responseMain.read()
    soupMain = BeautifulSoup(htmlMain, 'html.parser')
    bseTicker =  soupMain.find(id='Bse_Prc_tick')
    if(bseTicker!=None):
     if(float(bseTicker.contents[0].getText())<stockLimit and topStockGot == False):
      topStock.append(x)
      topStock.append(y)
      topStock.append(bseTicker.contents[0].getText())
      topStockGot = True;
     print "buy " + stockmap[x] + " priced at : " + bseTicker.contents[0].getText()
     totalItems = (math.floor(stockLimit/float(bseTicker.contents[0].getText())))
     totalCost = totalItems*float(bseTicker.contents[0].getText())
     moneyInHand = moneyInHand - totalCost
     print "bought " + str(math.floor(stockLimit/float(bseTicker.contents[0].getText())))
     print totalCost
     print moneyInHand 
     cursor.execute("insert into portfolio (userid, stockid, holdingdate,units, avgbuyprice, currentprice, targetprice)  values ('shubham',%s,date(now()),%s,%s,%s,%s)", (x,totalItems,float(bseTicker.contents[0].getText()),float(bseTicker.contents[0].getText()),0)) 
  except Exception as e:
    print e
    continue

if(moneyInHand>0):
 print "buy" + stockmap[topStock[0]] + " units:  " +str(math.floor(float(moneyInHand/float(topStock[2])))) 

db.commit()
db.close()


 
