from graphs import *
import time
import csv
import math
db = sys.argv[1]
d={}
table_e = 'contracted_ways'
table_v = 'contracted_ways_vertices_pgr'
d['db'] = db
d["table_e"] = table_e
d["table_v"] = table_v

conn = psycopg2.connect(database=d['db'], user="rohithreddy", password="postgres", host="127.0.0.1", port="5432")
d['conn'] = conn
cur = conn.cursor()
d['query'] = "SELECT source, target, cost, reverse_cost FROM "+table_e

d['directed'] = True


update_query = "UPDATE %s SET %s = %s + %s WHERE source = %s AND target = %s AND %s > 0"

current_time = time.time()
G = edge_table_to_graph(d)
print "Time taken to build graph: ", time.time()-current_time

num_iterations = 5

current_time = time.time()
k = 100

betweenness = {}
count = {}
for i in xrange(0, num_iterations):
	print "On iteration: ", i
	betweenness_temp = nx.edge_betweenness_centrality(G, k=k, weight = "weight", normalized=False)
	#count = 0
	for x in betweenness_temp.keys():
		try:
			betweenness[x] += betweenness_temp[x]
		except KeyError:
			betweenness[x] = betweenness_temp[x]
		try:
			count[x] += 1
		except KeyError:
			count[x] = 1


d['table'] = table_v
v_count = get_count(d)

print "k: ", k
print "Number of vertices: ", v_count

for x in betweenness.keys():
	betweenness[x] = math.ceil(betweenness[x]/count[x])

print "Inserting stuff in a csv file....."

ofile  = open(db+'_test'+'.csv', "wb")
writer = csv.writer(ofile, delimiter=',')


for key, value in betweenness.items():
	if value > 0.00:
		writer.writerow([key[0], key[1], value])
violate = 0

print "Inserting stuff in db....."

for edge in betweenness.keys():
	cur.execute(update_query, (AsIs("cleaned_ways"), AsIs("betweenness"), AsIs("betweenness"), betweenness[edge], edge[0], edge[1], AsIs("cost"), ))
	cur.execute(update_query, (AsIs("cleaned_ways"), AsIs("betweenness"), AsIs("betweenness"), betweenness[edge], edge[1], edge[0], AsIs("reverse_cost"), ))
	conn.commit()
#print "Time taken to calculate betweenness: ", time.time()-current_time
#print count