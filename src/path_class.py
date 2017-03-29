class Path:
	"""Class for extracting path differences"""
	
	def __init__(self):
		self.path = [];

	def get_path(self):
		return self.path;

	def add_entry(self, entry):
		self.path.append(entry);

	def add_path(self, p):
		if len(p) == 0:
			return;
		if len(self.path) == 0:
			for entry in p.get_path():
				self.path.append(entry);
		if self.get_last_node() == p.get_start_node():
			self.path.pop();
			for entry in p.get_path():
				self.path.append(entry);

	def get_path_cost(p):
		l = 0.00000;
		for entry in p.get_path():
			l = l + entry['cost'];
		return l;
	
	def get_edge_set(p):
		s = set();
		for entry in p:
			s.add(entry['edge']);
		return s;

	def get_node_set(p):
		s = set();
		for entry in p:
			s.add(entry['node']);
		return s;

	def get_distance_diff(self, path_1, path_2):
		return abs(self.get_path_len(path_1)-self.get_path_len(path_2));

	def get_edge_diff(self, path_1, path_2):
		s1 = self.get_edge_set(path_1);
		s2 = self.get_edge_set(path_2);
		u = s1.union(s2);
		i = s1.intersection(s2);
		return abs(len(i)*1.000/len(u));

	def get_node_diff(self, path_1, path_2):
		s1 = self.get_node_set(path_1);
		s2 = self.get_node_set(path_2);
		u = s1.union(s2);
		i = s1.intersection(s2);
		return abs(len(i)*1.000/len(u));

	def get_start_node(self):
		if len(self.path) > 0:
			return self.path[0]['node'];
		return -1;

	def get_last_node(self):
		if len(self.path) > 0:
			return self.path[-1]['node'];
		return -1;

	def reverse_path(self):
		self.path.reverse();
		
	def __str__(self):
		out = ""
		for entry in self.get_path():
			out = out + str(entry)+'\n';
		return out

	def __len__(self):
		return len(self.path)
		