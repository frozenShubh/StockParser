from cassandra.cluster import Cluster
import MySQLdb

cluster = Cluster()

# Open database connection
db = MySQLdb.connect("localhost","root","shubham","market")
cursor = db.cursor()

session = cluster.connect('market');
rows = session.execute('SELECT id , stockname , holdingdate , percentage , rank , category FROM market.holdings')
for user_row in rows:
    print user_row.category
    print cursor.execute("insert into market.holding (id, stockname, holdingdate, percentage, rank, category) values (%s,%s,now(),%s,%s,%s)", (user_row.id, user_row.stockname, user_row.percentage, int(user_row.rank), user_row.category))

db.commit()
