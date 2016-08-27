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


fundlist = []
fundlist.append('home.html')
from bs4 import BeautifulSoup
today = Set()
allstocks = dict()
mapofmaps = dict()
allCount = 0;
responseMain = urllib2.urlopen('http://www.moneycontrol.com/mutual-funds/performance-tracker/returns/large-cap.html')
htmlMain = responseMain.read()
soupMain = BeautifulSoup(htmlMain, 'html.parser')
divMain =  soupMain.find('div',class_='FL lsh')
stocknames = dict()

for linkMain in divMain.find_all('a'):
 print linkMain['href']
 thismap = dict()
##table header
 mapofstocks = dict()
 response = urllib2.urlopen('http://www.moneycontrol.com' + linkMain['href'])
 html = response.read()
 soup = BeautifulSoup(html, 'html.parser')
 countFund = 0;
 for link in soup.find_all('a',class_='bl_12'):
  print link['href'].split('/')[4]
  try:
   response1 = urllib2.urlopen('http://www.moneycontrol.com/india/mutualfunds/mfinfo/portfolio_holdings/'+link['href'].split('/')[4])
   holdinghtml = response1.read()
   soup1 = BeautifulSoup(holdinghtml, 'html.parser')
   tablelink = soup1.find_all('table',class_='tblporhd')
   #Equity 
   for stocklink in tablelink[0].children:
    try:
     if(stocklink.a['href'].split('/')[4]!='portfolio_holdings'):
      stock = stocklink.a['href'].split('/')[5]
      percentage = num(stocklink.contents[3].string)
      if(allstocks.has_key(stock)):
       allstocks[stock]=allstocks[stock]+percentage
      else:
       allstocks[stock]=percentage
      if(thismap.has_key(stock)):
       thismap[stock]=thismap[stock]+percentage
      else:
       thismap[stock]=percentage
      if(not stocknames.has_key(stock)):
       stocknames[stock]=stocklink.a['title']
       cursor.execute("insert into market.stocks(id,stockname,url) values (%s,%s,%s)",(stock,stocklink.a['title'],stocklink.a['href'])) 
    except Exception as es:
      continue; 
  except Exception as e:
   continue
  countFund=countFund+1
 mapofmaps[linkMain['href']]=thismap
 i =0
 print thismap
 for (x,y) in sorted(thismap.items(), key=operator.itemgetter(1), reverse=True):
  try:
   cursor.execute("insert into market.holding (id, holdingdate, percentage, rank, stockname, category) values (%s,CURDATE(),%s,%s,%s,%s)", (x,y/countFund,i,stocknames[x],linkMain['href'].split('/')[4]));
  except Exception as e:
   print e
   continue
  if(i<10):
   today.add(x)
  i = i + 1;
 print stocknames[x] + " : " + str(y/countFund)
 allCount+=countFund
for (x,y) in sorted(allstocks.items(), key=operator.itemgetter(1), reverse=True):
 print stocknames[x] + " : " + str(y/allCount)

db.commit();
db.close();
print today

