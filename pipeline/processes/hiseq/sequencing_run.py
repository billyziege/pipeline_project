import os
import time
import datetime
import sys
import re
import ConfigParser
from mockdb.initiate_mockdb import initiate_mockdb, save_mockdb
from processes.parsing import parse_sequencing_run_dir, get_sequencing_run_base_dir
from demultiplex_stats.extract_stats import extract_barcode_lane_stats, calculate_lane_total, calculate_weighted_percent


def fill_run_stats_from_directory_name(config,mockdb,directory,add=True):
    """
    This function reads in a directory and processes it.  If the flowcell exists,
    the sequencing run for the flowcell is pulled and overwritten.  If add
    is True or no flowcell exists, both the sequencing object and the flowcell are
    created.
    """
    [date,machine_key,run_number,side,flowcell_key] = parse_sequencing_run_dir(directory)
    last_name = date + "_" + machine_key + "_" + run_number + "_" + side + flowcell_key
    orig_filename = os.path.join(config.get('Common_directories','hiseq_output'),last_name,config.get('Filenames','basecalling_initialized'))
    done_filename = os.path.join(config.get('Common_directories','hiseq_output'),last_name,config.get('Filenames','basecalling_complete'))
    if os.path.isfile(orig_filename):
        #start_timestamp = datetime.datetime.strptime(time.ctime(os.path.getctime(orig_filename)),"%Y-%m-%d %H:%M:%S")
        start_timestamp = datetime.datetime.strptime(time.ctime(os.path.getctime(orig_filename)), "%a %b %d %H:%M:%S %Y")
    else:
        start_timestamp = None
    if os.path.isfile(done_filename):
        #end_timestamp = datetime.datetime.strptime(time.ctime(os.path.getctime(done_filename)),"%Y-%m-%d %H:%M:%S")
        end_timestamp = datetime.datetime.strptime(time.ctime(os.path.getctime(done_filename)), "%a %b %d %H:%M:%S %Y")
    else:
        end_timestamp = None
    machine = mockdb['HiSeqMachine'].__get__(config,key=machine_key)
    flowcell = mockdb['Flowcell'].__get__(config,key=flowcell_key)
    seq_run_flowcell_dict = mockdb['SequencingRun'].__attribute_value_to_object_dict__('flowcell_key')
    if add==False and flowcell_key in seq_run_flowcell_dict.keys():
        seq_runs = seq_run_flowcell_dict[flowcell_key]
        #if len(seq_runs) > 1:
            #raise Exception("A flowcell, {0}, has been used in multiple sequencing runs.".format(flowcell_key))
        for seq_run in seq_runs:
            seq_run.begin_timestamp = start_timestamp
            seq_run.end_timestamp = end_timestamp
            seq_run.flowcell_key = flowcell.key
            seq_run.machine_key = machine.key
    else:
        seq_run = mockdb['SequencingRun'].__new__(config,flowcell=flowcell,machine=machine,date=date,run_number=run_number,output_dir=directory,side=side,begin_timestamp=start_timestamp,end_timestamp=end_timestamp)
        seq_run.state = 'Complete'
    return 1

def determine_run_type(directory):
    """
    Looks at the SampleSheet.csv in the base sequencing directory
    and determines the number of lanes.  If 2, the run type is RapidRun.
    If 8, the run type is HighThroughputRun.  Otherwise, None
    is returned.
    """
    base_dir = get_sequencing_run_base_dir(directory)
    samplesheet_file = os.path.join(base_dir,"SampleSheet.csv")
    if not os.path.isfile(samplesheet_file):
        return None
    sample_sheet_table =  csv.DictReader(open(sample_sheet_file,delimiter=','))
    try:
        lane_numbers = set([row['Lane'] for row in table])
        if len(lane_numbers) == 2:
            return "RapidRun"
        if len(lane_numbers) == 8:
            return "HighThroughputRun"
    except:
        return None

if __name__ == "__main__":
    config = ConfigParser.ConfigParser()
    config.read('/home/sequencing/src/pipeline_project/pipeline/config/ihg_system.cfg')
    mockdb = initiate_mockdb(config)
    fill_run_stats_from_directory_name(config,mockdb,sys.argv[1],add=False)
    save_mockdb(config,mockdb)
    #print determine_run_type(sys.argv[1])
