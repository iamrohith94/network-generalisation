import psycopg2
import random
import math
from graphs import *

def is_table_present(parameters):
    """
    Checks whether a table with a particular name in the db exists or not
    """
    conn = parameters['conn']
    cur = conn.cursor()
    query = " SELECT table_name \
            FROM information_schema.tables \
            WHERE table_name = %s"
    cur.execute(query, (parameters['table'], ));
    rows = cur.fetchall();
    for row in rows:
        if row[0] == parameters['table']:
            return True
        else:
            return False
    return False



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
        edge_query = "SELECT id, source, target, cost, reverse_cost\
        FROM "+parameters['input_table'];
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
    e_query = "UPDATE %s SET %s = TRUE\
    WHERE  source = ANY(%s) OR target = ANY(%s)";
    v_query = "UPDATE %s SET %s = TRUE \
    WHERE id = ANY(%s)";
    cur.execute(e_query, (AsIs(parameters['table_e']),AsIs(parameters['contraction_column']), dead_end_vertices, dead_end_vertices));
    cur.execute(v_query, (AsIs(parameters['table_v']),AsIs(parameters['contraction_column']), dead_end_vertices,));

    conn.commit();
