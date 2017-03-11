# -*- coding: utf-8 -*-
"""
Created on Tue Feb 16 15:51:40 2016

@author: rohith
"""

import psycopg2
import numpy as np
from common import *;
from path_class import *;
from psycopg2.extensions import AsIs


"""
Returns the networkX graph structure of an edge table
row[0] -> source
row[1] -> target
row[2] -> cost
row[3] -> reverse_cost
"""   
def edge_table_to_graph(parameters):
    db = parameters['db']
    conn = parameters['conn']
    cur = conn.cursor();

    #query for vertex ids
    vertex_query = "SELECT id FROM "+parameters['table_v'];

    #query for edge ids
    edge_query = "SELECT source, target, cost, reverse_cost FROM "+parameters['table_e'];
    
    #Undirected graph or directed graph
    is_directed = parameters['directed'];
    if is_directed:
        G = nx.DiGraph();
    else:
        G = nx.Graph();
    
    #Adding vertices
    cur.execute(vertex_query);
    rows = cur.fetchall()
    for row in rows:
        G.add_node(row[0]);
    
    #Adding edges
    cur.execute(edge_query);
    rows = cur.fetchall()
    for row in rows:
        if row[2] > 0:
            G.add_edge(int(row[0]), int(row[1]))
        if row[3] > 0:
            G.add_edge(int(row[1]), int(row[0]))
    return G


"""
Returns the networkX graph structure
given queries for edge and vertex tables
row[0] -> source
row[1] -> target
row[2] -> cost
row[3] -> reverse_cost
"""   
def edge_table_to_graph_level(parameters):
    conn = parameters['conn']
    cur = conn.cursor();

    #query for vertex ids
    vertex_query = "SELECT id FROM %s WHERE %s <= %s";

    #query for edge ids
    edge_query = "SELECT source, target, cost, reverse_cost FROM %s WHERE %s <= %s";
    
    #Undirected graph or directed graph
    is_directed = parameters['directed'];
    if is_directed:
        G = nx.DiGraph();
    else:
        G = nx.Graph();
    
    #Adding vertices
    cur.execute(vertex_query,(AsIs(parameters['table_v']), AsIs(parameters['level_column']), parameters['level'], ));
    rows = cur.fetchall()
    for row in rows:
        G.add_node(row[0]);
    
    #Adding edges
    cur.execute(edge_query,(AsIs(parameters['table_e']), AsIs(parameters['level_column']), parameters['level'], ));
    rows = cur.fetchall()
    for row in rows:
        if row[2] > 0:
            G.add_edge(int(row[0]), int(row[1]))
        if row[3] > 0:
            G.add_edge(int(row[1]), int(row[0]))
    return G

def update_edges_to_lower_level(parameters):
    db = parameters['db'];
    table_e = parameters['table_e']
    table_v = parameters['table_v']
    parameters['query'] = "SELECT id FROM "+table_v+" WHERE level <= "+str(parameters['level']);
    skeleton_vertices = get_selected_columns(parameters);
    conn = parameters['conn']
    cur = conn.cursor()
    """
    query = "UPDATE %s SET level = \
    (SELECT level FROM %s \
    WHERE id = source) \
    WHERE (SELECT level FROM %s \
    WHERE id = source) = \
    (SELECT level FROM %s \
    WHERE id = target) \
    AND (SELECT level FROM %s \
    WHERE id = source) < level";
    """
    query = "UPDATE %s SET level = %s \
    WHERE source = ANY(%s) AND target = ANY(%s)";
    cur.execute(query, (AsIs(table_e), parameters['level'], skeleton_vertices, skeleton_vertices, ));
    conn.commit();

def get_skeleton(parameters):
    db = parameters['db'];
    conn = parameters['conn']
    cur = conn.cursor();
    query = "SELECT id, within_nodes FROM connected_components\
             ORDER BY ST_area(st_envelope(the_geom)) DESC \
             LIMIT 1";
    cur.execute(query);
    rows = cur.fetchall();
    for row in rows:
        skeleton_id = row[0];
        within_nodes = row[1];
    return skeleton_id, within_nodes;

def connect_components_to_skeleton(parameters):
    db = parameters['db'];
    table_e = parameters['table_e'];
    table_v = parameters['table_v'];
    level = parameters['level'];
    conn = parameters['conn']
    cur = conn.cursor();
    skeleton_id, skeleton_nodes = get_skeleton(parameters);
    largest_cc = set(skeleton_nodes);
    parameters['nodes'] = largest_cc;
    #print "Number of cc at level: "+str(level)+" are "+ str(len(connected_components))
    query = "SELECT id, within_nodes FROM connected_components\
     WHERE id != %s";
    cur.execute(query, (skeleton_id,));
    rows = cur.fetchall();
    for row in rows:
        nearest_vertex = -1;
        poi = -1;
        min_dist = 1000000;
        for v in row[1]:
            parameters['poi'] = v;
            vid, dist = get_nearest_node(parameters);
            if dist < min_dist:
                nearest_vertex = vid;
                min_dist = dist;
                poi = v;
        print "poi: ", poi;
        print "nearest_vertex: ", nearest_vertex;
        parameters['source'] = poi;
        parameters['target'] = nearest_vertex;
        update_promoted_level(parameters);
        largest_cc.union(set(row[1]));
    conn.commit();

def update_level_skeleton(parameters):
    conn = parameters['conn']
    cur = conn.cursor()
    paths_to_add = parameters['path_additions'];
    update_query = "UPDATE %s SET %s = %s WHERE id = ANY(%s)";
    edges_to_add = []
    nodes_to_add = []
    dijkstra_query = "SELECT node, edge from pgr_dijkstra(%s,%s,%s)"; 
    inner = 'SELECT id, source, target, cost, reverse_cost \
    FROM '+parameters['table_e']+' \
    WHERE is_contracted = FALSE';
    for edge in paths_to_add:
        src = edge[0]
        target = edge[1]
        #Adding the source and target into a table
        cur.execute(dijkstra_query, (inner, src, target, ));
        rows = cur.fetchall()
        for row in rows:
            nodes_to_add.append(row[0]);
            edges_to_add.append(row[1]);
    cur.execute(update_query, (AsIs(parameters['table_e']), AsIs(parameters['promoted_level_column']), parameters['level'], edges_to_add, ));
    cur.execute(update_query, (AsIs(parameters['table_v']), AsIs(parameters['promoted_level_column']), parameters['level'], nodes_to_add, ));

    conn.commit();
