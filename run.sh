psql -U postgres -d $1 -f generalisation/init.sql

python generalisation/clean.py $1
python generalisation/contraction.py $1
python generalisation/gen_centrality.py $1
python generalisation/gen_levels.py $1
python generalisation/gen_skeleton.py $1
#python generalisation/make_connected_comp.py $1
#python path_matching.py $1
