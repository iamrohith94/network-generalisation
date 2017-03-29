from shortest_path_class import *
db = "debug_gachibowli"
sp = ShortestPath(db);

levels = [10, 20, 30, 40, 50]
for level in levels:
	print level
	path = sp.get_generalised_path(84, 385, "promoted_level_"+str(level))
	print path.get_path_cost();
	print path
