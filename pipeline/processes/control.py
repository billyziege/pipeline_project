import os
import sys
import re
from processes.models import GenericProcess
from mockdb.models import FormattingError
from processes.hiseq.scripts import list_monitoring_dirs, list_sample_dirs
from processes.hiseq.sequencing_run import determine_run_type
from processes.transitions import things_to_do_if_zcat_complete, things_to_do_if_bcbio_complete, things_to_do_if_starting_pipeline
from processes.transitions import things_to_do_if_sequencing_run_is_complete, things_to_do_if_snps_called
from processes.transitions import things_to_do_if_bcbio_cleaning_complete, things_to_do_for_reports_object
from processes.transitions import things_to_do_if_snp_stats_complete
from manage_storage.disk_queries import disk_usage
from processes.parsing import parse_sequencing_run_dir
from sge_email.scripts import send_email

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
        run_type = determine_run_type(nd)
        seq_run=mockdb['SequencingRun'].__new__(config,flowcell=flowcell,machine=machine,date=date,run_number=run_number,output_dir=nd,side=side,run_type=run_type)
    return 1

def initialize_pipeline_for_finished_sequencing_runs(configs,storage_devices,mockdb,pipeline_name):
    """
    Checks running SequencingRuns and determines if they are complete.  If so, 
    the sequencing run is deligated to another function to continue with the next step. 
    """
    state_dict = mockdb['SequencingRun'].__attribute_value_to_object_dict__('state')
    try:
        for seq_run in state_dict['Running']:
            if seq_run.__is_complete__():
                things_to_do_if_sequencing_run_is_complete(configs,storage_devices,mockdb,seq_run,pipeline_name)
    except KeyError:
        pass
    return 1
    
def finish_seq_runs(mockdb):
    """
    Finishes the seq runs after pipelines have begun.
    """
    state_dict = mockdb['SequencingRun'].__attribute_value_to_object_dict__('state')
    problem_dirs = []
    try:
        for seq_run in state_dict['Running']:
            if seq_run.__is_complete__():
                sample_dirs = list_sample_dirs(seq_run.output_dir.split(":"))
                for sample in sample_dirs:
                    for sample_dir in sample_dirs[sample]:
                        if (int(disk_usage(sample_dir)) < 200000):
                            problem_dirs.append(sample_dir)
                seq_run.__finish__()
    except KeyError:
        pass
    if len(problem_dirs) > 0:
        message = "The following directory(ies) is less than 200MB:\n"
        for problem_dir in problem_dirs:
            message += "\t" + problem_dir + "\n"
        message += "Please check.\n"
        send_email("Small sample directory",message,recipients='zerbeb@humgen.ucsf.edu,LaoR@humgen.ucsf.edu')  
    return 1

def continue_backup_processes(configs,storage_devices,mockdb):
    """
    Checks running SequencingRuns and determines if they are complete.  If so, 
    the sequencing run is deligated to another function to continue with the next step. 
    """
    state_dict = mockdb['Backup'].__attribute_value_to_object_dict__('state')
    try:
        for backup in state_dict['Running']:
            if backup.__is_complete__(configs,storage_devices[backup.location]):
                backup.__finish__()
    except KeyError:
        pass
    return 1

def handle_automated_reports(configs,mockdb):
    """
    Looks for report objects, then finds any that are not yet complete, i.e.
    Running.  Then generates any necessary reports and sends them via e-mail.
    """
    for object_key in mockdb.keys():
        if not re.search("Reports$",object_key):
            continue
        state_dict = mockdb[object_key].__attribute_value_to_object_dict__('state')
        try:
            for object in state_dict['Running']:
                things_to_do_for_reports_object(configs,mockdb,object)
        except KeyError:
            pass
    return 1

def run_pipelines_with_enough_space(configs,storage_devices,mockdb,pipeline_class_name):
    """
    Identifies pipelines that are ready to run.  If they are ready, they are passed to
    a subfunction to determine if there is enough storage.
    """
    state_dict = mockdb[pipeline_class_name].__attribute_value_to_object_dict__('state')
    try:
        state_dict['Initialized']
    except KeyError:
        return 1
    for pipeline in state_dict['Initialized']:
        enough_space = run_pipeline_with_enough_space(configs,storage_devices,pipeline,mockdb)
        if enough_space != True:
            break
    return 1

def run_pipeline_with_enough_space(configs,storage_devices,pipeline,mockdb):
    """
    Checks to make sure there is enough space and that the first step of the
    pipeline hasn't been started.  Then passes the pipeline to another function
    where the next steps are intiated.
    """
    if hasattr(pipeline,"zcat_key") and pipeline.zcat_key != None:
        raise FormattingError("The pipeline has a zcat key but isn't initiated.")
    #if storage_devices[pipeline.running_location].__is_available__(configs['pipeline'].get('Storage','needed')):
    storage_devices[pipeline.running_location].my_use += int(configs['pipeline'].get('Storage','needed'))
    things_to_do_if_starting_pipeline(configs,mockdb,pipeline)
    return True
    #return False

def advance_running_qc_pipelines(configs,storage_devices,mockdb):
    """
    Identifies all pipelines that are currently running (if any).  Passes these
    pipelines to a subfunction to check the individual steps in the pipeline.
    """ 
    state_dict = mockdb['QualityControlPipeline'].__attribute_value_to_object_dict__('state')
    try:
        for pipeline in state_dict['Running']:
            advance_running_qc_pipeline(configs,storage_devices,pipeline,mockdb)
    except KeyError:
        pass
    return 1

def advance_running_std_pipelines(configs,storage_devices,mockdb,pipeline_name):
    """
    Same as the advance_running_qc_pipelines function, except for the StandardPipleine pipeline.
    """ 
    state_dict = mockdb[pipeline_name].__attribute_value_to_object_dict__('state')
    try:
        for pipeline in state_dict['Running']:
            pipeline.__copy_altered_parameters_to_config__(configs["pipeline"])
            if configs["pipeline"].has_option("Pipeline","steps"): #New interface for allowing external definition of linear pipelines
                pipeline.__handle_linear_steps__(configs,storage_devices,mockdb)
            else:
                advance_running_std_pipeline(configs,storage_devices,pipeline,mockdb)
    except KeyError:
        pass
    return 1

def advance_running_qc_pipeline(configs,storage_devices,pipeline,mockdb):
    """
    Determines which stage the pipeline currently is running, determines
    if that stage is complete, and then passes the relevant
    objects to subfunctions that handle the next step.
    

    This needs to be combined with the other advance the pipeline function... A more general framework
    where the process are stored.
    """
    if pipeline.zcat_key is None: #zcat hasn't begun, which should not be the case.
        pipeline.state = 'Initialized'
        raise FormattingError("The pipeline somehow began running before zcatting.")
    if pipeline.bcbio_key is None: #bcbio hasn't begun
        zcat = mockdb['Zcat'].__get__(configs['system'],key=int(pipeline.zcat_key))
        #print pipeline.zcat_key
        #print zcat.__is_complete__(config)
        if zcat.__is_complete__(configs):
            things_to_do_if_zcat_complete(configs,mockdb,pipeline,zcat)
        return 1
    if pipeline.snp_stats_key is None: #snp_stats hasn't begun
        bcbio = mockdb['Bcbio'].__get__(configs['system'],int(pipeline.bcbio_key))
        if bcbio.__snps_called__():
            things_to_do_if_snps_called(configs,mockdb,pipeline,bcbio)
        return 1
    if pipeline.cleaning_key is None: #cleaning hasn't begun
        bcbio = mockdb['Bcbio'].__get__(configs['system'],int(pipeline.bcbio_key))
        snp_stats = mockdb['SnpStats'].__get__(configs['system'],int(pipeline.snp_stats_key))
        #print pipeline.key
        #print bcbio.__is_complete__(configs)
        #print snp_stats.__is_complete__(configs,mockdb)
        if bcbio.__is_complete__(configs) and snp_stats.__is_complete__(configs,mockdb):
            things_to_do_if_bcbio_complete(configs,mockdb,pipeline,bcbio)
            things_to_do_if_snp_stats_complete(configs,mockdb,pipeline,snp_stats)
        return 1
    clean_bcbio = mockdb['CleanBcbio'].__get__(configs['system'],int(pipeline.cleaning_key))
    sys.stderr.write(pipeline.sample_key+"\n");
    sys.stderr.write(clean_bcbio.key+"\n")
    if clean_bcbio.__is_complete__():
        things_to_do_if_bcbio_cleaning_complete(storage_devices,mockdb,pipeline,clean_bcbio)
    return 1

def advance_running_std_pipeline(configs,storage_devices,pipeline,mockdb):
    """
    Same as the advance_running_qc_pipeline function, except for any completely linear pipeline.
    """ 
    if pipeline.zcat_key == None: #zcat hasn't begun, which should not be the case.
        pipeline.state = 'Initialized'
        raise FormattingError("The pipeline somehow began running before zcatting.")
    if pipeline.bcbio_key == None: #bcbio hasn't begun
        zcat = mockdb['Zcat'].__get__(configs['system'],key=int(pipeline.zcat_key))
        if zcat.__is_complete__():
            things_to_do_if_zcat_complete(configs,mockdb,pipeline,zcat)
        return 1
    if pipeline.cleaning_key is None: #cleaning hasn't begun
        bcbio = mockdb['Bcbio'].__get__(configs['system'],int(pipeline.bcbio_key))
        if bcbio.__is_complete__(configs):
            things_to_do_if_bcbio_complete(configs,mockdb,pipeline,bcbio)
        return 1
    clean_bcbio = mockdb['CleanBcbio'].__get__(configs['system'],int(pipeline.cleaning_key))
    if clean_bcbio.__is_complete__():
        things_to_do_if_bcbio_cleaning_complete(storage_devices,mockdb,pipeline,clean_bcbio)
    return 1

