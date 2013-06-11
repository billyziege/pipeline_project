from sge_queries.models import Node
from sge_queries.nodes import initialize_nodes, mark_broken, note_jobs, lightest_working

nodes = initialize_nodes()
nodes = mark_broken(nodes)
nodes = note_jobs(nodes)
min_nodes = lightest_working(nodes)
print min_nodes.keys()
print len(min_nodes.keys())
