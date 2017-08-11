import psycopg2
from psycopg2.extensions import AsIs
import sys

db = sys.argv[1];
table_e = 'cleaned_ways'
table_v = 'cleaned_ways_vertices_pgr'

d = {}
d["db"] = db;
motorway = 101
trunk = 104
primary = 106
secondary = 108
tertiary = 109
primary_link = 107
secondary_link = 124
tertiary_link = 125
popular_ways = [motorway, trunk, primary, secondary]

conn = psycopg2.connect(database=d['db'], user="postgres", password="postgres", host="127.0.0.1", port="5432")

levels = [10, 20, 30, 40, 50]

#total_count_sql = "SELECT count(*) FROM %s WHERE promoted_level_%s = 1";
total_count_sql = "SELECT count(*) FROM %s WHERE class_id = ANY(%s)"
popular_count_sql = "SELECT count(*) FROM %s WHERE promoted_level_%s = 1 AND class_id = ANY(%s)"
cur = conn.cursor()
cur.execute(total_count_sql, (AsIs(table_e), popular_ways, ))
rows = cur.fetchall()
for row in rows:
	total_count = row[0]
for level in levels:
	"""
	cur.execute(total_count_sql, (AsIs(table_e), level, ))
	rows = cur.fetchall()
	for row in rows:
		total_count = row[0]
	"""
	cur.execute(popular_count_sql, (AsIs(table_e), level, popular_ways, ))
	rows = cur.fetchall()
	for row in rows:
		popular_count = row[0]

	print "level: ", level
	#print "total_count: ", total_count
	#print "popular_count: ", popular_count
	print "percent of popular roads: ", (popular_count*1.00/total_count)*100