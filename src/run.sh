psql -U postgres -d $1 -f init.sql

python clean.py $1
python contraction.py $1
python gen_centrality_stats.py $1
python gen_levels.py $1
python gen_skeleton.py $1
#python path_matching.py $1
