import sys
import argparse
import ConfigParser
from mockdb.initiate_mockdb import initiate_mockdb,save_mockdb
from manage_storage.scripts import initiate_storage_devices, add_waiting_storage, add_running_storage
from processes.control import maintain_sequencing_run_objects, initialize_pipeline_for_finished_sequencing_runs,advance_running_qc_pipelines
from processes.control import advance_running_std_pipelines,run_pipelines_with_enough_space
from processes.control import continue_backup_processes, handle_automated_reports
from processes.transitions import things_to_do_if_initializing_pipeline_with_input_directory

parser = argparse.ArgumentParser(description='Manages data and submits new jobs.')
parser.add_argument('-i', dest='source_dir', help='fastq source', default=None)
parser.add_argument('-o', dest='dest_dir', help='vcf destination', default=None)
parser.add_argument('-p', '--pipeline', dest='pipeline', help='The version of the pipeline', default='QualityControlPipeline')
parser.add_argument('-c', '--config', dest='config_file', help='The configuration file', default='/mnt/iscsi_space/zerbeb/pipeline_project/pipeline/config/qc.cfg')
options = parser.parse_args()

#Load config
config = ConfigParser.ConfigParser()
config.read(options.config_file)

#Initialize and load mockdb
mockdb=initiate_mockdb(config)

#Grab storage devices to keep track of storage
storage_devices = initiate_storage_devices(config)
add_waiting_storage(config,storage_devices,mockdb)

#Either maintain mockdbs and identify new directories or simply import directories
if options.source_dir != None and options.dest_dir != None:
    things_to_do_if_initializing_pipeline_with_input_directory(config,storage_devices,mockdb,options.source_dir,options.dest_dir)
else:
    maintain_sequencing_run_objects(config,mockdb)
    initialize_pipeline_for_finished_sequencing_runs(config,storage_devices,mockdb)

#Complete any backup process that have been initiated by a finished sequencing run.
continue_backup_processes(config,storage_devices,mockdb)

#Advance the running pipelines.  If a step is done, preceed to the next.  If the pipeline is done, complete it.
print "Advancing"
advance_running_qc_pipelines(config,storage_devices,mockdb)
advance_running_std_pipelines(config,storage_devices,mockdb)

#The remaining pipeline are taking up storage.  Account for this
print "Adding running storage"
add_running_storage(config,storage_devices,mockdb)

#If there is enough space, moce the next pipeline into the queue
print "Adding new pipelines"
run_pipelines_with_enough_space(config,storage_devices,mockdb,'QualityControlPipeline')
run_pipelines_with_enough_space(config,storage_devices,mockdb,'StandardPipeline')

#Generate and send any outstanding reports.
handle_automated_reports(config,mockdb)

save_mockdb(config,mockdb)
