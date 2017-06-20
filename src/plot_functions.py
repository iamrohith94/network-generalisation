import matplotlib.pyplot as plt
from psycopg2.extensions import AsIs
import psycopg2

def betweenness_distribution(parameters):
	"""
	Draws a plot between the edge_count(betweenness) 
	VS the number of edges(having a particular betweenness)
	"""
	conn = parameters['conn']
	cur = conn.cursor()
	betweenness_values = sorted(parameters['betweenness_values'])
	edge_count = []
	query = "SELECT count(*) FROM %s WHERE %s = %s"
	for value in betweenness_values:
		cur.execute(query, (AsIs(parameters['table_e']), AsIs(parameters['column']), value))
		rows = cur.fetchall()
		for row in rows:
			edge_count.append(row[0])
	plt.xlabel('Betweenness')
	plt.ylabel('Edge count')
	plt.plot(betweenness_values, edge_count, 'ro')
	plt.savefig('../images/betweenness_distribution.png',facecolor='white')