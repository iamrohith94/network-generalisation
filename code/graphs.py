# -*- coding: utf-8 -*-
"""
Created on Tue Feb 16 15:51:40 2016

@author: rohith
"""

import psycopg2
import random
import math
import matplotlib.pyplot as plt
import numpy as np
from common import *;

def generate_random_pairs(parameters):
    vertex_map={}
    vertex_pairs=[]
    fraction=parameters['fraction']
    size=parameters['size'];
    vertex_count = parameters['vertex_count']
    size=int(size*fraction)
    while(len(vertex_pairs)!=size):
        #print len(vertex_pairs),size
        v1=random.randint(1,vertex_count)
        v2=random.randint(1,vertex_count)
        if v1==v2:
            pass
        if (v1,v2) not in vertex_map and (v2,v1) not in vertex_map:
            vertex_map[(v1,v2)]=1
            vertex_map[(v2,v1)]=1
            vertex_pairs.append((v1,v2))
    return vertex_pairs


def add_count_column(parameters):
    db=parameters['db']
    table = parameters['table']
    conn = psycopg2.connect(database=db, user="postgres", password="postgres", host="127.0.0.1", port="5432")    
    cur = conn.cursor()
    #cur.execute("ALTER TABLE "+table+" ADD COLUMN edge_count bigint default 0")
    cur.execute("SELECT column_name \
               FROM information_schema.columns \
               WHERE table_schema='public' and table_name='"+table+"' \
               AND column_name='edge_count'");
               
    rows = cur.fetchall();
    col = ""
    for row in rows:
        col = row[0]
    if col != "edge_count": 
        cur.execute("ALTER TABLE "+table+" ADD COLUMN edge_count bigint default 0");
    conn.commit()
    conn.close()
    
def add_distance_column(parameters):
    db=parameters['db']
    conn = psycopg2.connect(database=db, user="postgres", password="postgres", host="127.0.0.1", port="5432")    
    cur = conn.cursor()
    cur.execute("ALTER TABLE ways ADD COLUMN edge_distance double precision default 0.000")
    conn.commit()
    conn.close()

def generate_count(parameters):
    vertex_pairs=parameters['vertex_pairs']
    db = parameters['db']
    table = parameters['table']
    conn = psycopg2.connect(database=db, user="postgres", password="postgres", host="127.0.0.1", port="5432")    
    cur = conn.cursor()
    count={}
    etab={}
    etab["db"] = db;
    etab["table"] = table;
    edge_count=get_count(etab)
    for x in xrange(0,edge_count+1):
        count[int(x)]=0
    for pair in vertex_pairs:
        #print pair
        s=pair[0]
        t=pair[1]
        inner = 'SELECT id,source,target,cost FROM '+table;
        cur.execute("SELECT edge from pgr_dijkstra(%s,%s,%s,false)", (inner, s, t ));
        rows = cur.fetchall()
        for row in rows:
            eid=int(row[0])
            #print eid	
            if eid !=-1:
                count[eid]=count[eid]+1
    conn.close()
    return count
    
def insert_count(parameters):
    count=parameters['count']
    db=parameters['db']
    table=parameters['table']
    conn = psycopg2.connect(database=db, user="postgres", password="postgres", host="127.0.0.1", port="5432")    
    cur = conn.cursor()
    for eid in count.keys():
        cur.execute("UPDATE "+table+" SET edge_count="+str(count[eid])+" WHERE id= "+str(eid))
    conn.commit()
    conn.close()    


def generate_edge_distance(parameters):
    edge_distance={}
    db=parameters['db']
    table=parameters['table']
    mean=get_vertex_centroid(parameters)
    mean_x=mean[0]
    mean_y=mean[1]
    #print "vertex centroid: ",mean
    conn = psycopg2.connect(database=db, user="postgres", password="postgres", host="127.0.0.1", port="5432")    
    cur = conn.cursor()
    cur.execute("SELECT gid ,ST_X((ST_Centroid(the_geom))),ST_Y((ST_Centroid(the_geom))) from "+table)
    rows=cur.fetchall()
    for row in rows:
        eid=row[0]
        #print "eid: ",eid
        x=row[1]
        y=row[2]
        edge_distance[eid]=math.sqrt( math.pow(mean_x-x,2)+ math.pow(mean_y-y,2))
    return edge_distance
    
def insert_distances(parameters):
    edge_distance=parameters['edge_distance']
    db=parameters['db']
    conn = psycopg2.connect(database=db, user="postgres", password="postgres", host="127.0.0.1", port="5432")    
    cur = conn.cursor()
    for eid in edge_distance.keys():
        cur.execute("UPDATE ways SET edge_distance="+str(edge_distance[eid])+" WHERE gid= "+str(eid))
    conn.commit()
    conn.close()

    
def create_new_ways(parameters):
    db=parameters['db']
    fraction=parameters['fraction']
    percent=int(fraction*100)
    conn = psycopg2.connect(database=db, user="postgres", password="postgres", host="127.0.0.1", port="5432")    
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS ways_"+str(percent))
    cur.execute("SELECT * INTO ways_"+str(percent)+" FROM ways" )
    conn.commit()
    conn.close()

"""d={}
d["db"]="gachibowli_2"
d["table"]="ways_vertices_pgr"
vertex_count=get_count(d);
d["vertex_count"]=vertex_count
d["size"]=vertex_count*vertex_count
d["table"]="ways"
#print d["vertex_count"]
d["fraction"]=0.3
#print generate_random_pairs(d)
#add_count_column(d)
#add_distance_column(d)
d["vertex_pairs"]=generate_random_pairs(d)
d["count"]= generate_count(d)
d["column"]="the_geom"
d["srid"]=32643
#print d["count"]
insert_count(d)
#change_vertices_geometry(d)
#change_edges_geometry(d)
d["edge_distance"]=generate_edge_distance(d)
#print max(d["edge_distance"].values())
insert_distances(d)
create_new_ways(d)

x=np.array(d["edge_distance"].values())
y=np.array(d["count"].values())
sorted_y=sorted(y)
#plt.plot(x,y,'ro')
plt.plot(sorted_y)
#plt.show() """
