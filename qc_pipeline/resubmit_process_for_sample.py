import sys
import argparse
import ConfigParser
from mockdb.initiate_mockdb import initiate_mockdb,save_mockdb
from manage_storage.models import StorageDevice
from manage_storage.scripts import initiate_storage_devices, add_waiting_storage, add_running_storage
from processes.control import maintain_sequencing_run_objects, initialize_pipeline_for_finished_sequencing_runs,advance_running_qc_pipelines,run_qc_pipelines_with_enough_space
from processes.transitions import things_to_do_if_initializing_pipeline_with_input_directory

parser = argparse.ArgumentParser(description='Manages data and submits new jobs.')
parser.add_argument('--sample', dest='sample_key', help='sample_name', default=None)
parser.add_argument('--process', dest='process', help='process name (like bcbio or zcat)', default='Bcbio')
options = parser.parse_args()

#Load config
config = ConfigParser.ConfigParser()
config.read('/mnt/iscsi_space/zerbeb/qc_pipeline_project/qc_pipeline/config/qc.cfg')

#Initialize and load mockdb
mockdb=initiate_mockdb(config)

#Either maintain mockdbs and identify new directories or simply import directories
if options.sample_key == None:
    sys.exit("A sample key must be defined.")
else:
    sample_dict = mockdb['QualityControlPipeline'].__attribute_value_to_object_dict__('sample_key')
    for pipeline in sample_dict[options.sample_key]:
        if options.process == 'Bcbio':
            mockdb['Bcbio'].objects[pipeline.bcbio_key].__launch__(config)
    #object.__launch__(config)

save_mockdb(config,mockdb)
