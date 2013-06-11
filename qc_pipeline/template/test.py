import sys
from mako.template import Template
from template.scripts import find_fields,fill_template

fname = '/mnt/iscsi_speed/zerbeb/qc_pipeline_project/qc_pipeline/storage/templates/zcat.template'
keys = '/mnt/iscsi_speed/zerbeb/qc_pipeline_project/qc_pipeline/keys'
#with open(fname,'r') as f:
#    print "\n".join(set(find_fields(f.read())))
d = {}
with open(keys,'r') as f:
    for line in f:
        k,v = line.split()
        d.update({k:v})
print fill_template(fname,d)

#fields = ['list_of_r1_files', 'out_dir', 'r1_file', 'stderr', 'list_of_r2_files', 'r2_file', 'complete_file','extra']
#vals = ['r1_list', 'output', 'r1_file', 'stderr_file', 'r2_list', 'r2_file', 'complete', 'nowhere']
#tmpl = Template(filename=fname)
#dictionary = dict(zip(fields, vals))
#print fill_template(tmpl,dictionary)
