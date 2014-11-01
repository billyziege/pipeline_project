import os
import sys
import re
import csv
from processes.hiseq.scripts import list_sample_dirs
from processes.hiseq.sample_sheet import clean_sample_name, clean_index


#This function reads the sample sheet into appropriate objects
def parse_sample_sheet(config,mockdb,directory):
    table =  csv.DictReader(open(os.path.join(directory, 'SampleSheet.csv')),delimiter=',')
    samplesheet={}
    for row in table:
        samplesheet = row
    if not 'SampleID' in samplesheet:
        sys.exit("No file SampleSheet.csv in " + directory) 
    parsed = {}
    #sys.stderr.write(str(samplesheet))
    #sample_key = translate_sample_name(samplesheet['SampleID'])
    sample_key = clean_sample_name(samplesheet['SampleID'])
    parsed['project_name'] = samplesheet['SampleProject']
    parsed['sample'] = mockdb['Sample'].__get__(config,key=sample_key)
    flowcell_key = samplesheet['FCID']
    parsed['flowcell'] = mockdb['Flowcell'].__get__(config,key=flowcell_key)
    lane_key = flowcell_key + '_lane_' + samplesheet['Lane']
    parsed['lane'] = mockdb['Lane'].__get__(config,key=lane_key,flowcell=parsed['flowcell'],number=samplesheet['Lane'])
    barcode_key = lane_key + '_' + clean_index(samplesheet['Index'])
    parsed['barcode'] = mockdb['Barcode'].__get__(config,key=barcode_key,sample=parsed['sample'],lane=parsed['lane'],project=parsed['project_name'],index=samplesheet['Index'])
    description = sample_key + "_" + samplesheet['Description']
    parsed['description'] = description
    try:
        parsed['sample_ref'] = samplesheet['SampleRef']
    except:
        parsed['sample_ref'] = None
    try:
        parsed['operator'] = samplesheet['Operator']
    except:
        parsed['operator'] = None
    try:
        parsed['input_amount'] = samplesheet['input_amount']
    except:
        parsed['input_amount'] = None
    try:
        parsed['yield_from_library'] = samplesheet['yield_from_library']
    except:
        parsed['yield_from_library'] = None
    try:
        parsed['amount_bp'] = samplesheet['amount_bp']
    except:
        parsed['amount_bp'] = None
    parsed['recipe'] = samplesheet['Recipe']
    return parsed

def parse_sequencing_run_dir(directory):
    base_dir = get_sequencing_run_base_dir(directory)
    (head,tail) = os.path.split(base_dir)
    names = tail.split("_")
    if len(names) < 4:
        raise Exception("Improper directory name {0}.".format(directory))
    date = names[0]
    machine = names[1]
    run_number = names[2]
    side = names[3][0:1]
    flowcell = names[3][1:]
    return [date,machine,run_number,side,flowcell]

def get_sequencing_run_base_dir(directory):
    (head,tail) = os.path.split(re.sub("/$","",directory))
    if re.search("Sample",tail):
        (head,tail) = os.path.split(head)
    if re.search("Project",tail):
        (head,tail) = os.path.split(head)
    if re.search("Basecall_Stats",tail):
        (head,tail) = os.path.split(head)
    return os.path.join(head,tail)

