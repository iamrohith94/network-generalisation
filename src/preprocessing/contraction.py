#Contracting the cleaned data
from common import *
from graphs import *


def generate_contraction_results(parameters):
    """
    Stores the contraction results into a table if the results are
    not yet generated for the graph
    """
    conn = parameters['conn']
    contraction_table = parameters['contraction_table']
    parameters['table'] = contraction_table;
    #If table is already present it doesn't compute contraction
    if is_table_present(parameters) == False:  
        cur = conn.cursor()
        edge_query = "SELECT id, source, target, cost, reverse_cost "\
        "FROM "+parameters['input_table'];
        query = "SELECT * INTO %s FROM pgr_contractGraph(%s, %s)";
        cur.execute(query, (AsIs(contraction_table), edge_query, parameters['contraction_order'], ));
        conn.commit();
    
def update_contraction_results(parameters):
    """
    Updates the contracted results in the original table
    """
    conn = parameters['conn']
    cur = conn.cursor()

    input_table_e = parameters['table_e']
    input_table_v = parameters['table_v']

    #Retrieving the dead end vertices
    dead_end_vertices = []
    dead_end_query = "select array_agg(c) FROM \
    (SELECT unnest(contracted_vertices) \
    FROM %s) as dt(c)";
    cur.execute(dead_end_query, (AsIs(parameters['contraction_table']), ));
    rows = cur.fetchall();
    for row in rows:
    	dead_end_vertices = row[0]
    dead_end_vertices = [int(x) for x in dead_end_vertices]

    #Updating the edges and vertices of the graph with contraction results
    e_query = "UPDATE %s SET %s = TRUE "\
    "WHERE  source = ANY(%s) OR target = ANY(%s)";
    v_query = "UPDATE %s SET %s = TRUE "\
    "WHERE id = ANY(%s)";
    cur.execute(e_query, (AsIs(parameters['table_e']),AsIs(parameters['contraction_column']), dead_end_vertices, dead_end_vertices));
    cur.execute(v_query, (AsIs(parameters['table_v']),AsIs(parameters['contraction_column']), dead_end_vertices,));

    conn.commit();



if __name__ == '__main__':
	d = {}
	d['db'] = sys.argv[1];
	table_e = "cleaned_ways"
	table_v = "cleaned_ways_vertices_pgr"
	d['table'] = table_e;
	conn = psycopg2.connect(database=d['db'], user="rohithreddy", password="postgres", host="127.0.0.1", port="5432")
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