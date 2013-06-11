import os
import time
import sys
import re
import ConfigParser
from mockdb.initiate_mockdb import initiate_mockdb, save_mockdb
from processes.parsing import parse_sequencing_run_dir
from demultiplex_stats.extract_stats import extract_barcode_lane_stats, calculate_lane_total, calculate_weighted_percent


def fill_run_stats_from_directory_name(config,mockdb,directory,add=False):
    """
    This function reads in a directory and processes it.  If the flowcell exists,
    the sequencing run for the flowcell is pulled and overwritten.  If add
    is True or no flowcell exists, both the sequencing object and the flowcell are
    created.
    """
    last_name = os.path.basename(re.sub("/$","",directory))
    orig_filename = os.path.join(config.get('Common_directories','hiseq_output'),last_name,config.get('Filenames','basecalling_initialized'))
    done_filename = os.path.join(config.get('Common_directories','hiseq_output'),last_name,config.get('Filenames','basecalling_complete'))
    if os.path.isfile(orig_filename):
        start = time.ctime(os.path.getctime(orig_filename))
        start_timestamp, start_num = re.subn("\s+","_",start)
    else:
        start_timestamp = None
    if os.path.isfile(done_filename):
        end = time.ctime(os.path.getctime(done_filename))
        end_timestamp, end_num = re.subn("\s+","_",end)
    else:
        end_timestamp = None
    [date,machine_key,run_number,side,flowcell_key] = parse_sequencing_run_dir(directory)
    machine = mockdb['HiSeqMachine'].__get__(config,key=machine_key)
    flowcell = mockdb['Flowcell'].__get__(config,key=flowcell_key)
    seq_run_flowcell_dict = mockdb['SequencingRun'].__attribute_value_to_object_dict__('flowcell_key')
    if add==False and flowcell_key in seq_run_flowcell_dict.keys():
        seq_runs = seq_run_flowcell_dict[flowcell_key]
        if len(seq_runs) > 1:
            raise Exception("A flowcell, {0}, has been used in multiple sequencing runs.".format(flowcell_key))
        seq_run = seq_runs[0]
    else:
        seq_run = mockdb['SequencingRun'].__new__(config,flowcell=flowcell,machine=machine,date=date,run_number=run_number,output_dir=directory,side=side,begin_timestamp=start_timestamp,end_timestamp=end_timestamp)
        seq_run.state = 'Complete'
    return 1

if __name__ == "__main__":
    config = ConfigParser.ConfigParser()
    config.read('/mnt/iscsi_space/zerbeb/qc_pipeline_project/qc_pipeline/config/qc.cfg')
    mockdb = initiate_mockdb(config)
    fill_run_stats_from_directory_name(config,mockdb,sys.argv[1],add=True)
    save_mockdb(config,mockdb)
