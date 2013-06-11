import subprocess
import ConfigParser
import random
import re
from sge_queries.models import Node

def initialize_nodes():
    nodes = {}
    command = ["qstat", "-f"]
    proc = subprocess.Popen(command,stdout=subprocess.PIPE)
    out = proc.stdout.read().split("\n")
    for line in out:
        match_obj = re.search('(\w+)@(node\w+)',line)
        if match_obj:
            nodes[match_obj.group(2)] = Node(match_obj.group(2))
    return nodes

def mark_broken(nodes,config):
    command = ["qstat", "-f"]
    proc = subprocess.Popen(command,stdout=subprocess.PIPE)
    out = proc.stdout.read().split("\n")
    for line in out:
        match_obj = re.search('(\w+)@(node\w+)',line)
        if not match_obj:
            continue
	columns = line.split()
        if len(columns) < 6:
            continue
        if columns[5] == "":
            continue
        nodes[match_obj.group(2)].broken = True
    for name in config.get('SGE','do_not_use_nodes').split(','):
        nodes[name].broken = True
    return nodes    

def note_jobs(nodes):
    command = ["qstat", "-u", "*"]
    proc = subprocess.Popen(command,stdout=subprocess.PIPE)
    out = proc.stdout.read().split("\n")
    for line in out:
        match_obj = re.search('(\w+)@(node\w+)',line)
        if not match_obj:
            continue
        if nodes[match_obj.group(2)].number_jobs == 'Inf':
            continue
        if match_obj.group(1) == 'single':
            nodes[match_obj.group(2)].number_jobs = 'Inf'
            continue
        nodes[match_obj.group(2)].number_jobs += 1
    return nodes

def minimal_jobs(nodes):
    min_jobs = 999
    for name,node in nodes.iteritems():
        if node.number_jobs == 'Inf':
            continue
        if node.broken == True:
            continue
        if node.number_jobs < min_jobs:
            min_jobs = node.number_jobs
    return min_jobs

def lightest_working(nodes):
    min_jobs = minimal_jobs(nodes)
    min_nodes = {}
    for name,node in nodes.iteritems():
        if node.broken == True:
            continue
        if node.number_jobs == min_jobs:
            min_nodes.update({name:node})
    return min_nodes
    
def grab_good_node(config):
    nodes = initialize_nodes()
    nodes = mark_broken(nodes,config)
    nodes = note_jobs(nodes)
    min_nodes = lightest_working(nodes) 
    return random.choice(min_nodes.keys())
    
if __name__ == '__main__':
    config = ConfigParser.ConfigParser()
    config.read('/mnt/iscsi_space/zerbeb/qc_pipeline_project/qc_pipeline/config/qc.cfg')
    print grab_good_node(config)
