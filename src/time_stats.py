import matplotlib.pyplot as plt
from shortest_path_class import *
from common import *
db = sys.argv[1];
table_e = 'cleaned_ways'
table_v = 'cleaned_ways_vertices_pgr'

d = {}
d["db"] = db;
d["table_e"] = table_e
d["table_v"] = table_v
conn = psycopg2.connect(database=d['db'], user="rohithreddy", password="postgres", host="127.0.0.1", port="5432")
d['conn'] = conn;
d['num_pairs'] = 1000
cur = conn.cursor()

#store_random_pairs_into_table(d)

equal = 0;
non_equal = 0;
limit = 1000
num_iterations = 2
time_table = "time_stats"
edges_sql = "SELECT id, source, target, cost, reverse_cost FROM cleaned_ways"
pairs_sql = "SELECT source, target FROM random_pairs LIMIT %s"
contracted_time_sql = "SELECT source, target, build_time, avg_execution_time FROM pgr_conn_compQuery_time(%s, %s, %s, %s, %s, %s)"
insert_sql_contracted = "INSERT INTO %s(source, target, level, contracted_build_time, contracted_avg_execution_time) VALUES(%s, %s, %s, %s, %s)"
orig_time_sql = "SELECT source, target, build_time, avg_execution_time FROM pgr_timeAnalysis(%s, %s, %s, %s, %s)"
update_sql = "UPDATE %s SET actual_build_time = %s, actual_avg_execution_time = %s WHERE source = %s AND target = %s"
cur.execute(pairs_sql, (limit, ))

pairs = cur.fetchall()
sources = []
targets = []
count = 0

for pair in pairs:
	if count % 100 == 1:
		print count
		#print sources
		#print targets
		levels = [10, 20, 30, 40, 50]
		for level in levels:
			print level
			cur.execute(contracted_time_sql, (table_e, table_v, sources, targets, level, num_iterations))
			rows = cur.fetchall()
			for row in rows:
				#print "source: ", row[0]
				#print "target: ", row[1]
				cur.execute(insert_sql_contracted, (AsIs(time_table), row[0], row[1], level, row[2], row[3]))
				conn.commit()
		cur.execute(orig_time_sql, (edges_sql, 'pgr_dijkstra', sources, targets, num_iterations))
		rows = cur.fetchall()
		#print "Actual Times"
		for row in rows:
			#print "source: ", row[0]
			#print "target: ", row[1]
			#print row[2], row[3]
			cur.execute(update_sql, (AsIs(time_table), row[2], row[3], row[0], row[1]))
			conn.commit()
		conn.commit()
		sources = []
		targets = []
	
	sources.append(pair[0])
	targets.append(pair[1])
	count += 1

levels = [10, 20, 30, 40, 50]
for level in levels:
	cur.execute(contracted_time_sql, (table_e, table_v, sources, targets, level, num_iterations))
	rows = cur.fetchall()
	for row in rows:
		cur.execute(insert_sql_contracted, (AsIs(time_table), row[0], row[1], level, row[2], row[3]))

cur.execute(orig_time_sql, (edges_sql, 'pgr_dijkstra', sources, targets, num_iterations))
rows = cur.fetchall()
for row in rows:
	cur.execute(update_sql, (AsIs(time_table), row[2], row[3], row[0], row[1]))
"""
for pair in pairs:
	sources.append(pair[0])
	targets.append(pair[1])
levels = [10, 20, 30, 40, 50]
for level in levels:
	cur.execute(contracted_time_sql, (table_e, table_v, sources, targets, level, num_iterations))
	rows = cur.fetchall()
	for row in rows:
		cur.execute(insert_sql_contracted, (AsIs(time_table), row[0], row[1], level, row[2], row[3]))

cur.execute(orig_time_sql, (edges_sql, 'pgr_dijkstra', sources, targets, num_iterations))
rows = cur.fetchall()
for row in rows:
	cur.execute(update_sql, (AsIs(time_table), row[2], row[3], row[0], row[1]))
"""
conn.commit()
