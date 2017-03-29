#Contracting the cleaned data
from common import *
from graphs import *
from contraction_functions import *
d = {}
d['db'] = sys.argv[1];
table_e = "cleaned_ways"
table_v = "cleaned_ways_vertices_pgr"
d['table'] = table_e;
conn = psycopg2.connect(database=d['db'], user="postgres", password="postgres", host="127.0.0.1", port="5432")
d['conn'] = conn

"""
1 -> dead end contraction
2 -> linear contraction
"""
d['contraction_order'] = [1]
d['input_table'] = table_e;
d['contraction_table'] = "contraction_results";

print "Generating contraction results......"
generate_contraction_results(d);

print "Generating contracted graph......"
d['table_e'] = table_e;
d['table_v'] = table_v;
d['directed'] = True;
d['contraction_column'] = "is_contracted"
update_contraction_results(d);


#Storing the contraction results in separate tables namely contracted_ways and contracted_ways_vertices_pgr
d['query'] = "SELECT id, source, target, cost, reverse_cost, the_geom INTO contracted_ways FROM cleaned_ways WHERE is_contracted = FALSE";
run_query(d);

d['query'] = "SELECT * INTO contracted_ways_vertices_pgr FROM cleaned_ways_vertices_pgr WHERE is_contracted = FALSE";
run_query(d);


d['table'] = 'contracted_ways'
print "Contracted edge count: " + str(get_count(d));

d['table'] = 'contracted_ways_vertices_pgr'
print "Contracted vertex count: " + str(get_count(d));


conn.commit()
conn.close();