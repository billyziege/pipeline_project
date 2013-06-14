import sys
import subprocess
from config.scripts import get_location_dictionary_from_config
from manage_storage.models import StorageDevice
from manage_storage.disk_queries import disk_usage

#This function adds the needed storage to the running total for
#all running instances of a pipeline process.
def add_running_storage(config,storage_devices,mockdb):
    state_dict = mockdb['QualityControlPipeline'].__attribute_value_to_object_dict__('state')
    buffer_storage = int(config.get('Storage','buffer'))
    try:
        for pipeline in state_dict['Running']:
            currently_used = storage_currently_used_by_pipeline(config,mockdb,pipeline)
            additional_needed = int(pipeline.storage_needed) - int(currently_used)
            if additional_needed < buffer_storage:
                storage_devices[pipeline.running_location].my_use += buffer_storage
            else:
                storage_devices[pipeline.running_location].my_use += additional_needed
    except KeyError:
        pass
    return 1

def add_waiting_storage(config,storage_devices,mockdb):
    state_dict = mockdb['QualityControlPipeline'].__attribute_value_to_object_dict__('state')
    needed_storage = int(config.get('Storage','needed'))
    try:
        for pipeline in state_dict['Initialized']:
            storage_devices[pipeline.running_location].waiting += needed_storage
    except KeyError:
        pass
    return 1

def storage_currently_used_by_pipeline(config,mockdb,pipeline):
    if pipeline.bcbio_key != None:
        bcbio = mockdb['Bcbio'].__get__(config,pipeline.bcbio_key)
        return disk_usage(bcbio.output_dir)
    elif pipeline.zcat_key != None:
        zcat = mockdb['Zcat'].__get__(config,pipeline.zcat_key)
        return disk_usage(zcat.output_dir)
    return 0

def identify_running_location_with_most_currently_available(config,storage_devices):
    best_location = None
    largest_available = None
    needed_storage = int(config.get('Storage','needed'))
    for location, storage_device in storage_devices.iteritems():
        current_available = storage_device.available - storage_device.waiting
        if best_location == None:
            best_location = location
            largest_available = current_available
            continue
        if current_available > largest_available:
            best_location = location
            largest_available = current_available
    storage_devices[best_location].waiting += needed_storage
    return best_location

def initiate_storage_devices(config):
    location_dirs = get_location_dictionary_from_config(config)
    storage_devices = {}
    for name, directory in location_dirs.iteritems():
        storage_devices.update({name:StorageDevice(directory=directory,name=name,limit=config.get('Storage','limit'))})
    backup_dir = config.get("Backup","dir")
    name = config.get("Backup","dir_name")
    storage_devices.update({name:StorageDevice(directory=directory,name=name)})
    return storage_devices



if __name__ == '__main__':
    print disk_usage(sys.argv[1])
