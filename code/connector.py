#!/usr/bin/python

import psycopg2

conn = psycopg2.connect(database="gachibowli_1", user="postgres", password="postgres", host="127.0.0.1", port="5432")

print "Opened database successfully"


cur = conn.cursor()
cur.execute("SELECT count(*)  from ways")
rows = cur.fetchall()
for row in rows:
	edge_count=row[0]
   
cur.execute("SELECT count(*)  from ways_vertices_pgr")
rows = cur.fetchall()
for row in rows:
	vertex_count=row[0]

print "edge count = ", edge_count,"\n"
print "vertex count = ", vertex_count,"\n"

cur.execute('''DROP TABLE IF EXISTS edge_count ''')

cur.execute('''CREATE TABLE edge_count
       (id INT PRIMARY KEY     NOT NULL,
       count            INT     NOT NULL);''')

count={}

for x in xrange(1,edge_count+1):
	count[x]=0


for v1 in xrange(1,vertex_count+1):
	for v2 in xrange(1,vertex_count+1):
		if v1!=v2:
			#print v1,v2
			cur.execute("SELECT edge from pgr_dijkstra('SELECT gid as id,source,target,cost,reverse_cost FROM ways',"+str(v1)+","+str(v2)+")")
			rows = cur.fetchall()
			for row in rows:
				eid=row[0]
			#	print eid	
				if eid !=-1:
					count[eid]=count[eid]+1

for x in xrange(1,edge_count+1):
	print "eid: ",x," count: ",count[x]

for x in xrange(1,edge_count+1):
	cur.execute("INSERT INTO edge_count (id,count) \
      VALUES ("+str(x)+","+str(count[x]) +")")

cur.execute('''SELECT * into ways_count \
				FROM ways \
				INNER JOIN edge_count \
				ON ways.gid = edge_count.id;''')

cur.execute('''ALTER TABLE ways_count DROP id;''')

print "Operation done successfully";

conn.commit()
conn.close()