import sys
import ConfigParser
from mockdb.initiate_mockdb import initiate_mockdb, save_mockdb
from processes.parsing import parse_sequencing_run_dir
from demultiplex_stats.fill_demultiplex_stats import fill_demultiplex_stats


directory = sys.argv[1]
config = ConfigParser.ConfigParser()
config.read('/mnt/iscsi_space/zerbeb/qc_pipeline_project/qc_pipeline/config/qc.cfg')

mockdb=initiate_mockdb(config)

[date,machine_key,run_number,side,flowcell_key] = parse_sequencing_run_dir(directory)
machine = mockdb['HiSeqMachine'].__get__(config, machine_key)
flowcell = mockdb['Flowcell'].__get__(config, flowcell_key)
fill_demultiplex_stats(config,mockdb,directory,flowcell,machine)

save_mockdb(config,mockdb)
