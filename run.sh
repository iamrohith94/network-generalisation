#Creating a database
dropdb --if-exists $1

#Creating a database
createdb $1

#Installing postGIS
psql -U postgres -d $1 -c "CREATE EXTENSION postgis"

#Importing OSM data
osm2pgrouting -f /home/rohithreddy/mystuff/research/data/$1.osm -c /usr/share/osm2pgrouting/mapconfig_for_cars.xml -d $1 -U postgres -W postgres

psql -U postgres -d $1 -f generalisation/init.sql

python generalisation/clean.py $1

#Create index on id, source, target
psql -U postgres -d $1 -c "CREATE INDEX gen_ind ON cleaned_ways (id, source, target);"
python generalisation/contraction.py $1 $2
python generalisation/gen_centrality.py $1 $2
python generalisation/gen_levels.py $1
python generalisation/gen_skeleton.py $1
python generalisation/partition.py $1
