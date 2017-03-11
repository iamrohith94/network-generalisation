#Cleaning the OSM data by removing small disconnectivities in the network  
from common import *
from graphs import *
import sys
d = {}
database = sys.argv[1];
d['db'] = database;
d['table_e'] = "ways";
d['table_v'] = "ways_vertices_pgr";
conn = psycopg2.connect(database=d['db'], user="postgres", password="postgres", host="127.0.0.1", port="5432")
d['conn'] = conn


d['table'] = 'ways'
print "Initial edge count: " + str(get_count(d));

d['table'] = 'ways_vertices_pgr'
print "Initial vertex count: " + str(get_count(d));

print "Cleaning data......."
clean_data(d);

d['table'] = 'cleaned_ways'
print "Edge count after Cleaning: " + str(get_count(d));

d['table'] = 'cleaned_ways_vertices_pgr'
print "Vertex count after Cleaning: " + str(get_count(d));

conn.close();