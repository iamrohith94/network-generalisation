# -*- coding: utf-8 -*-
"""
Created on Tue May 10 14:17:22 2016

@author: rohithreddy
"""
from graphs import *
d={}
d["db"]="paper"
d["fraction"] = 0.5
tables = ["barbell_graph", "complete_graph",
          "complete_bipartite_graph", "grid_graph",
          "ladder_graph", "wheel_graph", "balanced_tree" 
          ];
for table in tables:          
    d["table"] = table+"_vertices_pgr"
    vertex_count = get_count(d);
    d["vertex_count"] = vertex_count
    d["size"] = vertex_count*vertex_count
    d["table"] = table;
    print d["vertex_count"]
    #d["fraction"]=0.3
    #print generate_random_pairs(d)
    print "Adding count column"
    add_count_column(d)
    #add_distance_column(d)
    print "generating pairs...."
    d["vertex_pairs"]=generate_random_pairs(d)
    print "generating count...."
    d["count"]= generate_count(d)
    d["column"]="the_geom"
    d["srid"]=32643
    #print d["count"]
    insert_count(d)
    #change_vertices_geometry(d)
    #change_edges_geometry(d)
    #d["edge_distance"]=generate_edge_distance(d)
    #print max(d["edge_distance"].values())
    #insert_distances(d)
    #create_new_ways(d)

"""x=np.array(d["edge_distance"].values())
y=np.array(d["count"].values())
sorted_y=sorted(y)
#plt.plot(x,y,'ro')
plt.plot(sorted_y)
#plt.show() """
