import os
import sys
import re
from processes.models import GenericProcess
from mockdb.models import FormattingError
from processes.hiseq.scripts import list_monitoring_dirs
from processes.transitions import things_to_do_if_zcat_complete, things_to_do_if_bcbio_complete, things_to_do_if_starting_pipeline
from processes.transitions import things_to_do_if_sequencing_run_is_complete, things_to_do_if_snps_called
from processes.transitions import things_to_do_if_bcbio_cleaning_complete
from processes.parsing import parse_sequencing_run_dir

def maintain_sequencing_run_objects(config,mockdb):
    """
    Reads in the directories in the casava output directory and compares it to what's in the 
    SequencingRun database.  If it is a new directory, a new sequencing run object is created in
    the Running state.
    """
    monitoring_dirs = set(list_monitoring_dirs(config.get('Common_directories','casava_output')))
    db_dirs = set(mockdb['SequencingRun'].__attribute_value_to_key_dict__('output_dir').keys())
    new_dirs = monitoring_dirs.difference(db_dirs)
    for nd in new_dirs:
        try:
            [date,machine_key,run_number,side,flowcell_key] = parse_sequencing_run_dir(nd)
        except:
            date = None
            machine_key = "dummy_machine"
            flowcell_key = "dummy_flowcell"
            run_number = -1
            side = "dummy_side"
        machine = mockdb['HiSeqMachine'].__get__(config,key=machine_key)
        flowcell = mockdb['Flowcell'].__get__(config,key=flowcell_key)
        seq_run=mockdb['SequencingRun'].__new__(config,flowcell=flowcell,machine=machine,date=date,run_number=run_number,output_dir=nd,side=side)
    return 1

def initialize_pipeline_for_finished_sequencing_runs(config,storage_devices,mockdb):
    """
    Checks running SequencingRuns and determines if they are complete.  If so, 
    the sequencing run is deligated to another function to continue with the next step. 
    """
    state_dict = mockdb['SequencingRun'].__attribute_value_to_object_dict__('state')
    try:
        for seq_run in state_dict['Running']:
            if seq_run.__is_complete__():
                things_to_do_if_sequencing_run_is_complete(config,storage_devices,mockdb,seq_run)
    except KeyError:
        pass
    return 1
    
def continue_backup_processes(config,storage_devices,mockdb):
    """
    Checks running SequencingRuns and determines if they are complete.  If so, 
    the sequencing run is deligated to another function to continue with the next step. 
    """
    state_dict = mockdb['Backup'].__attribute_value_to_object_dict__('state')
    try:
        for backup in state_dict['Running']:
            if backup.__is_complete__(config,storage_devices[backup.location]):
                backup.__finish__()
    except KeyError:
        pass
    return 1

def run_pipelines_with_enough_space(config,storage_devices,mockdb,pipeline_class_name):
    """
    Identifies pipelines that are ready to run.  If they are ready, they are passed to
    a subfunction to determine if there is enough storage.
    """
    state_dict = mockdb['SequencingRun'].__attribute_value_to_object_dict__('state')
    state_dict = mockdb[pipeline_class_name].__attribute_value_to_object_dict__('state')
    try:
        state_dict['Initialized']
    except KeyError:
        return 1
    for pipeline in state_dict['Initialized']:
        enough_space = run_pipeline_with_enough_space(config,storage_devices,pipeline,mockdb)
        if enough_space != True:
            break
    return 1

def run_pipeline_with_enough_space(config,storage_devices,pipeline,mockdb):
    """
    Checks to make sure there is enough space and that the first step of the
    pipeline hasn't been started.  Then passes the pipeline to another function
    where the next steps are intiated.
    """
    if pipeline.zcat_key != None:
        raise FormattingError("The pipeline has a zcat key but isn't initiated.")
    if storage_devices[pipeline.running_location].__is_available__(config.get('Storage','needed'))):
        things_to_do_if_starting_pipeline(config,mockdb,pipeline)
        return True
    return False

def advance_running_qc_pipelines(config,storage_devices,mockdb):
    """
    Identifies all pipelines that are currently running (if any).  Passes these
    pipelines to a subfunction to check the individual steps in the pipeline.
    """ 
    state_dict = mockdb['QualityControlPipeline'].__attribute_value_to_object_dict__('state')
    try:
        for pipeline in state_dict['Running']:
            advance_running_qc_pipeline(config,storage_devices,pipeline,mockdb)
    except KeyError:
        pass
    return 1

def advance_running_std_pipelines(config,storage_devices,mockdb):
    """
    Same as the advance_running_qc_pipelines function, except for the StandardPipleine pipeline.
    """ 
    state_dict = mockdb['StandardPipeline'].__attribute_value_to_object_dict__('state')
    try:
        for pipeline in state_dict['Running']:
            advance_running_std_pipeline(config,storage_devices,pipeline,mockdb)
    except KeyError:
        pass
    return 1

def advance_running_qc_pipeline(config,storage_devices,pipeline,mockdb):
    """
    Determines which stage the pipeline currently is running, determines
    if that stage is complete, and then passes the relevant
    objects to subfunctions that handle the next step.
    """
    if pipeline.zcat_key == None: #zcat hasn't begun, which should not be the case.
        pipeline.state = 'Initialized'
        raise FormattingError("The pipeline somehow began running before zcatting.")
    if pipeline.bcbio_key == None: #bcbio hasn't begun
        zcat = mockdb['Zcat'].__get__(config,key=int(pipeline.zcat_key))
        if zcat.__is_complete__(config):
            things_to_do_if_zcat_complete(config,mockdb,pipeline,zcat)
        return 1
    if pipeline.snp_stats_key is None: #snp_stats hasn't begun
        bcbio = mockdb['Bcbio'].__get__(config,int(pipeline.bcbio_key))
        if bcbio.__snps_called__():
            things_to_do_if_snps_called(config,mockdb,pipeline,bcbio)
        return 1
    if pipeline.cleaning_key is None: #cleaning hasn't begun
        bcbio = mockdb['Bcbio'].__get__(config,int(pipeline.bcbio_key))
        snp_stats = mockdb['SnpStats'].__get__(config,int(pipeline.snp_stats_key))
        if bcbio.__is_complete__(config) and snp_stats.__is_complete__(config,mockdb):
            things_to_do_if_bcbio_complete(config,mockdb,pipeline,bcbio)
            snp_stats.__finish__()
        return 1
    clean_bcbio = mockdb['CleanBcbio'].__get__(config,int(pipeline.cleaning_key))
    if clean_bcbio.__is_complete__(config):
        things_to_do_if_bcbio_cleaning_complete(storage_devices,mockdb,pipeline,clean_bcbio)
    return 1

def advance_running_std_pipeline(config,storage_devices,pipeline,mockdb):
    """
    Same as the advance_running_qc_pipeline function, except for the StandardPipleine pipeline.
    """ 
    if pipeline.zcat_key == None: #zcat hasn't begun, which should not be the case.
        pipeline.state = 'Initialized'
        raise FormattingError("The pipeline somehow began running before zcatting.")
    if pipeline.bcbio_key == None: #bcbio hasn't begun
        zcat = mockdb['Zcat'].__get__(config,key=int(pipeline.zcat_key))
        if zcat.__is_complete__(config):
            things_to_do_if_zcat_complete(config,mockdb,pipeline,zcat)
        return 1
    if pipeline.cleaning_key is None: #cleaning hasn't begun
        bcbio = mockdb['Bcbio'].__get__(config,int(pipeline.bcbio_key))
        if bcbio.__is_complete__(config):
            things_to_do_if_bcbio_complete(config,mockdb,pipeline,bcbio)
        return 1
    clean_bcbio = mockdb['CleanBcbio'].__get__(config,int(pipeline.cleaning_key))
    if clean_bcbio.__is_complete__(config):
        things_to_do_if_bcbio_cleaning_complete(storage_devices,mockdb,pipeline,clean_bcbio)
    return 1

