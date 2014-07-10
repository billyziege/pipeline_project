import sys
import argparse
import ConfigParser
from mockdb.initiate_mockdb import initiate_mockdb,save_mockdb
from manage_storage.scripts import initiate_storage_devices, add_waiting_storage, add_running_storage
from processes.control import maintain_sequencing_run_objects, initialize_pipeline_for_finished_sequencing_runs,advance_running_qc_pipelines
from processes.control import advance_running_std_pipelines,run_pipelines_with_enough_space
from processes.control import continue_backup_processes, handle_automated_reports, finish_seq_runs
from processes.transitions import things_to_do_if_initializing_pipeline_with_input_directory

parser = argparse.ArgumentParser(description='Manages data and submits new jobs.')
parser.add_argument('-i', dest='source_dir', help='fastq source', default=None)
parser.add_argument('-o', dest='dest_dir', help='vcf destination', default=None)
parser.add_argument('-p', '--pipeline', dest='pipeline', help='The version of the pipeline', default='QualityControlPipeline')
parser.add_argument('--system_config', dest='system_config_file', help='The system configuration file', default='/home/sequencing/src/pipeline_project/pipeline/config/ihg_system.cfg')
options = parser.parse_args()

#Load configs
system_config = ConfigParser.ConfigParser()
system_config.read(options.system_config_file)
pipelines = system_config.get('Pipeline','opts').split(',')
pipeline_config = {}
for pipeline_name in pipelines:
    config_instance = ConfigParser.ConfigParser()
    pipeline_config.update({pipeline_name:config_instance})
    pipeline_config[pipeline_name].read(system_config.get('Pipeline',pipeline_name))
configs = {}
configs.update({'system':system_config})
#Initialize and load mockdb
mockdb=initiate_mockdb(system_config)

#Grab storage devices to keep track of storage
storage_devices = initiate_storage_devices(system_config)
for pipeline_name in pipeline_config.keys():
    add_waiting_storage(pipeline_config[pipeline_name],storage_devices,mockdb,pipeline_name)
#Either maintain mockdbs and identify new directories or simply import directories
if options.source_dir != None and options.dest_dir != None:
    configs.update({'pipeline':pipeline_config[options.pipeline]})
    things_to_do_if_initializing_pipeline_with_input_directory(configs,storage_devices,mockdb,options.source_dir,base_output_dir=options.dest_dir,pipeline_name=options.pipeline)
else:
    maintain_sequencing_run_objects(system_config,mockdb)
    for pipeline_name in pipeline_config.keys():
        configs.update({'pipeline':pipeline_config[pipeline_name]})
        initialize_pipeline_for_finished_sequencing_runs(configs,storage_devices,mockdb,pipeline_name)
    finish_seq_runs(mockdb)

#Complete any backup process that have been initiated by a finished sequencing run.
#for pipeline_name in pipeline_config.keys():
#    configs.update({'pipeline':pipeline_config[pipeline_name]})
#    continue_backup_processes(configs,storage_devices,mockdb)

#Advance the running pipelines.  If a step is done, preceed to the next.  If the pipeline is done, complete it.
configs.update({'pipeline':pipeline_config['QualityControlPipeline']})
advance_running_qc_pipelines(configs,storage_devices,mockdb)
configs.update({'pipeline':pipeline_config['StandardPipeline']})
advance_running_std_pipelines(configs,storage_devices,mockdb,'StandardPipeline')
configs.update({'pipeline':pipeline_config['MHCPipeline']})
advance_running_std_pipelines(configs,storage_devices,mockdb,'MHCPipeline')
configs.update({'pipeline':pipeline_config['RD2Pipeline']})
advance_running_std_pipelines(configs,storage_devices,mockdb,'RD2Pipeline')
configs.update({'pipeline':pipeline_config['DevelPipeline']})
advance_running_std_pipelines(configs,storage_devices,mockdb,'DevelPipeline')
configs.update({'pipeline':pipeline_config['BBPipeline']})
advance_running_std_pipelines(configs,storage_devices,mockdb,'BBPipeline')
configs.update({'pipeline':pipeline_config['KanePipeline']})
advance_running_std_pipelines(configs,storage_devices,mockdb,'KanePipeline')
configs.update({'pipeline':pipeline_config['NGv3PlusPipeline']})
advance_running_std_pipelines(configs,storage_devices,mockdb,'NGv3PlusPipeline')

#The remaining pipeline are taking up storage.  Account for this
add_running_storage(configs['system'],storage_devices,mockdb)

#If there is enough space, move the next pipeline into the queue
for pipeline_name in pipeline_config:
    configs.update({'pipeline':pipeline_config[pipeline_name]})
    run_pipelines_with_enough_space(configs,storage_devices,mockdb,pipeline_name)
#Generate and send any outstanding reports.
configs.update({'pipeline':pipeline_config['QualityControlPipeline']})
handle_automated_reports(configs,mockdb)

save_mockdb(configs['system'],mockdb)
