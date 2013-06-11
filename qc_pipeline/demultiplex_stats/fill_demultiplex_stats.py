import os
import time
import sys
import re
import argparse
import ConfigParser
from mockdb.initiate_mockdb import initiate_mockdb, save_mockdb
from processes.parsing import parse_sequencing_run_dir
from demultiplex_stats.extract_stats import extract_barcode_lane_stats, calculate_lane_total, calculate_weighted_percent
from processes.hiseq.scripts import translate_sample_name

def fill_demultiplex_stats(config,mockdb,directory,flowcell,machine):
    """
    Pulls the information form the demultiplex stats file
    and puts it in the correct objects.  The directory
    in the arguments is the base output directory for casava,
    i.e. it contains the Project_PROJECT_ID directories as well as the 
    Basecall_Stats_FCID directory.
    """
    flowcell_key = flowcell.key
    machine_key = machine.key
    demultiplex_filename = os.path.join(directory, config.get('Common_directories','basecall_front')+ flowcell_key,config.get('Filenames','demultiplex'))
    if not os.path.isfile(demultiplex_filename):
        return
    struct = extract_barcode_lane_stats(demultiplex_filename)
    total_reads = calculate_lane_total(struct,'# Reads')
    pf = calculate_weighted_percent(struct,'% PF')
    q30 = calculate_weighted_percent(struct,'% of &gt;= Q30 Bases (PF)') 
    for i in range(0,len(struct)):
        number = struct[i]['Lane']
        lane_key = flowcell_key + '_lane_' + number
        lane = mockdb['Lane'].__get__(config,key=lane_key,flowcell=flowcell,number=number)
        lane.total_reads = total_reads[number]
        lane.percentage_pf = pf[number]
        lane.percentage_above_q30 = q30[number]
        sample_key = translate_sample_name(struct[i]['Sample ID'])
        if re.search('lane',sample_key):
            lane.undetermined_reads = struct[i]['# Reads']
            continue
        sample = mockdb['Sample'].__get__(config,key=sample_key)
        index = struct[i]['Index']
        barcode_key = lane_key + "_" + index
        barcode = mockdb['Barcode'].__get__(config,key=barcode_key,sample=sample,index=index,lane=lane)
        barcode.reads = struct[i]['# Reads']
    return 1

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Places the statistics in Demultiplex_Stats.html into the database')
    parser.add_argument('directory', type=str, help='The directory name where the casava results are placed.')
    args = parser.parse_args()
    config = ConfigParser.ConfigParser()
    config.read('/mnt/iscsi_space/zerbeb/qc_pipeline_project/qc_pipeline/config/qc.cfg')
    mockdb = initiate_mockdb(config)
    [date,machine_key,run_number,side,flowcell_key] = parse_sequencing_run_dir(args.directory)
    flowcell = mockdb['Flowcell'].objects[flowcell_key]
    machine = mockdb['HiSeqMachine'].objects[machine_key]
    fill_demultiplex_stats(config,mockdb,args.directory,flowcell,machine)
    save_mockdb(config,mockdb)
