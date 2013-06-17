import sys
import subprocess
from config.scripts import get_location_dictionary_from_config
from manage_storage.models import StorageDevice
from manage_storage.disk_queries import disk_usage

#This function adds the needed storage to the running total for
#all running instances of a pipeline process.
def add_running_storage(config,storage_devices,mockdb):
    """
    Wraps functions that keep track of the needed space on storage
    devices for running processes.
    """
    add_pipeline_running_storage(config,storage_devices,mockdb,'QualityControlPipeline')
    add_pipeline_running_storage(config,storage_devices,mockdb,'StandardPipeline')
    add_backup_running_storage(config,storage_devices,mockdb,'Backup')
    return 1

def add_pipeline_running_storage(config,storage_devices,mockdb,pipeline_name):
    """
    Pipeline are moved to a processing directory and then expand as intermediate
    data is generated.  This function keeps track of the expected storage use 
    that these intermediate files will require.
    """
    state_dict = mockdb[pipeline_name].__attribute_value_to_object_dict__('state')
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

def add_backup_running_storage(config,storage_devices,mockdb,backup_name):
    """
    Backing up across slow connections takes time, and this function
    keeps track of the expected requirements placed on the backup device
    by backup jobs that are currently running.
    """
    state_dict = mockdb[backup_name].__attribute_value_to_object_dict__('state')
    try:
        for backup in state_dict['Running']:
            storage_devices[backup.location].my_use += config.get('Storage','required_fastq_size')
    except KeyError:
        pass
    return 1

def add_waiting_storage(config,storage_devices,mockdb):
    """
    This keeps track of how much storage is currently required for Initiated jobs
    assigned to a processing location but that have been prevented from running due
    to space concerns. 
    """
    state_dict = mockdb['QualityControlPipeline'].__attribute_value_to_object_dict__('state')
    needed_storage = int(config.get('Storage','needed'))
    try:
        for pipeline in state_dict['Initialized']:
            storage_devices[pipeline.running_location].waiting += needed_storage
    except KeyError:
        pass
    return 1

def storage_currently_used_by_pipeline(config,mockdb,pipeline):
    """
    Determines the amount of storage currently used by the pipeline's output
    directory.
    """
    if pipeline.bcbio_key != None:
        bcbio = mockdb['Bcbio'].__get__(config,pipeline.bcbio_key)
        return disk_usage(bcbio.output_dir)
    elif pipeline.zcat_key != None:
        zcat = mockdb['Zcat'].__get__(config,pipeline.zcat_key)
        return disk_usage(zcat.output_dir)
    return 0

def identify_running_location_with_most_currently_available(config,storage_devices):
    """
    Returns the device from the list of available devices that
    has the most available storage.
    """
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
    """
    Load the storage device objects into memory and initializes
    import attributes.
    """
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
