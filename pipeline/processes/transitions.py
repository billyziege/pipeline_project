import os
from config.scripts import MyConfigParser
import sys
import re
import datetime
from processes.parsing import parse_sample_sheet
from processes.hiseq.scripts import list_monitoring_dirs, list_sample_dirs
from processes.hiseq.sequencing_run import determine_run_type
from manage_storage.scripts import identify_running_location_with_most_currently_available
from manage_storage.disk_queries import disk_usage
from demultiplex_stats.fill_demultiplex_stats import fill_demultiplex_stats
from processes.summary_stats.extract_stats import store_snp_stats_in_db, store_search_stats_in_db
from processes.pipeline.extract_stats import store_stats_in_db
from sge_email.scripts import send_email
from processes.hiseq.multi_fastq import create_multi_fastq_yaml
from mockdb.scripts import translate_underscores_to_capitals

def begin_next_step(configs,mockdb,pipeline,step_objects,next_step_key,prev_step_key):
    """The begin functions are to be used with linear pipelines"""
    try:
        step_objects[next_step_key] = globals()["begin_"+next_step_key](configs,mockdb,pipeline,step_objects,prev_step_key=prev_step_key)
    except KeyError:
        step_objects[next_step_key] = begin_generic_step(configs,mockdb,pipeline,step_objects,next_step_key,prev_step_key)
    return step_objects

def begin_generic_step(configs,mockdb,pipeline,step_objects,next_step_key,prev_step_key):
    """
    After working on this for a while, I noticed that most steps do the same exact things when begun.  This function generalizes this.
    """
    if hasattr(pipeline, "sample_key"):
        sys.stderr.write("Beginning "+next_step_key+" for "+pipeline.sample_key+"\n")
        return begin_generic_sample_step(configs,mockdb,pipeline,step_objects,next_step_key,prev_step_key)
    return begin_generic_unlabelled_step(configs,mockdb,pipeline,step_objects,next_step_key,prev_step_key)

def begin_generic_unlabelled_step(configs,mockdb,pipeline,step_objects,next_step_key,prev_step_key):
    """
    """
    next_step_obj = mockdb[translate_underscores_to_capitals(next_step_key)].__new__(configs['system'],pipeline_config=configs["pipeline"],pipeline=pipeline,prev_step=step_objects[prev_step_key])
    if configs["system"].get("Logging","debug") is "True":
       print "  "+translate_underscores_to_capitals(next_step_key)+": " + str(next_step_obj.key) 
    setattr(pipeline,next_step_key+"_key",next_step_obj.key)
    next_step_obj.__fill_qsub_file__(configs)
    if configs["system"].get("Logging","debug") is "True":
        print("Qsub file should be filled in "+next_step_obj.qsub_file)
    next_step_obj.__launch__(configs['system'])
    return next_step_obj

def begin_generic_sample_step(configs,mockdb,pipeline,step_objects,next_step_key,prev_step_key):
    """
    """
    sample = mockdb['Sample'].__get__(configs['system'],pipeline.sample_key)
    if configs["system"].get("Logging","debug") is "True":
       print "  Sample: " + sample.key 
    next_step_obj = mockdb[translate_underscores_to_capitals(next_step_key)].__new__(configs['system'],sample=sample,prev_step=step_objects[prev_step_key],pipeline_config=configs["pipeline"],pipeline=pipeline)
    if configs["system"].get("Logging","debug") is "True":
       print "  "+translate_underscores_to_capitals(next_step_key)+": " + str(next_step_obj.key) 
    setattr(pipeline,next_step_key+"_key",next_step_obj.key)
    next_step_obj.__fill_qsub_file__(configs)
    if configs["system"].get("Logging","debug") is "True":
        print("Qsub file should be filled in "+next_step_obj.qsub_file)
    next_step_obj.__launch__(configs['system'])
    return next_step_obj

def begin_zcat_multiple(configs,mockdb,pipeline,step_objects,**kwargs):
    sample = mockdb['Sample'].__get__(configs['system'],pipeline.sample_key)
    project_out = re.sub('_','-',pipeline.project)
    if re.search("[0-9]",project_out[0:1]):
        project_out = "Project-" + project_out
    section_header = pipeline.running_location + '_directories'
    base_output_dir = configs['pipeline'].get(section_header,'working_directory')
    date=datetime.date.today().strftime("%Y%m%d")
    output_dir = os.path.join(base_output_dir,project_out + "_" + sample.key + '_' + str(date))
    if not os.path.isdir(output_dir) and not re.search('dummy',output_dir):
        os.makedirs(output_dir)
    multi_fastq_file = os.path.join(output_dir,sample.key + '_fastq.yaml')
    create_multi_fastq_yaml(multi_fastq_file,pipeline.input_dir.split(":"))
    zcat_multiple = mockdb['ZcatMultiple'].__new__(configs['system'],sample=sample,multi_fastq_file=multi_fastq_file,date=date,output_dir=output_dir)
    pipeline.zcat_multiple_key = zcat_multiple.key
    zcat_multiple.__fill_qsub_file__(configs)
    zcat_multiple.__launch__(configs['system'])
    return zcat_multiple

def begin_cat(configs,mockdb,pipeline,step_objects,**kwargs):
    sample = mockdb['Sample'].__get__(configs['system'],pipeline.sample_key)
    project_out = re.sub('_','-',pipeline.project)
    if re.search("[0-9]",project_out[0:1]):
        project_out = "Project-" + project_out
    section_header = pipeline.running_location + '_directories'
    base_output_dir = configs['pipeline'].get(section_header,'working_directory')
    date=datetime.date.today().strftime("%Y%m%d")
    output_dir = os.path.join(base_output_dir,project_out + "_" + sample.key + '_' + str(date))
    if not os.path.isdir(output_dir) and not re.search('dummy',output_dir):
        os.makedirs(output_dir)
    cat = mockdb['Cat'].__new__(configs['system'],sample=sample,date=date,input_dir=pipeline.input_dir,output_dir=output_dir)
    pipeline.cat_key = cat.key
    cat.__fill_qsub_file__(configs)
    cat.__launch__(configs['system'])
    return cat

def begin_bcbio(configs,mockdb,pipeline,step_objects,**kwargs):
    zcat = step_objects["zcat"] #Some of the parameters of the bcbio are dependent on the parameters in the zcat object.
    sample = mockdb['Sample'].__get__(configs['system'],pipeline.sample_key)
    flowcell = mockdb['Flowcell'].__get__(configs['system'],pipeline.flowcell_key)
    section_header = pipeline.running_location + '_directories'
    base_output_dir = configs['pipeline'].get(section_header,'working_directory')
    try:
        capture_target_bed = pipeline.capture_target_bed
    except:
        capture_target_bed = None
    bcbio = mockdb['Bcbio'].__new__(configs['system'],sample=sample,flowcell=flowcell,base_output_dir=base_output_dir,r1_path=zcat.r1_path,r2_path=zcat.r2_path,upload_dir=pipeline.output_dir,description=pipeline.description,output_dir=zcat.output_dir,capture_target_bed=capture_target_bed)
    pipeline.bcbio_key = bcbio.key
    bcbio.__fill_all_templates__(configs)
    bcbio.__launch__(configs['system'])
    return bcbio

def begin_summary_stats(configs,mockdb,pipeline,step_objects,**kwargs):
    sample = mockdb['Sample'].__get__(configs['system'],pipeline.sample_key)
    try:
        capture_target_bed = pipeline.capture_target_bed
    except:
        capture_target_bed = None
    summary_stats = mockdb['SummaryStats'].__new__(configs['system'],sample=sample,bcbio=step_objects["bcbio"],capture_target_bed=capture_target_bed)
    summary_stats.__fill_qsub_file__(configs)
    pipeline.summary_stats_key = summary_stats.key
    summary_stats.__launch__(configs['system'])
    return summary_stats

def begin_clean_bcbio(configs,mockdb,pipeline,step_objects,**kwargs):
    sample = mockdb['Sample'].__get__(configs['system'],pipeline.sample_key)
    clean_bcbio = mockdb['CleanBcbio'].__new__(configs['system'],sample=sample,bcbio=step_objects["bcbio"],input_dir=step_objects["bcbio"].output_dir,output_dir=pipeline.output_dir,process_name='clean')
    clean_bcbio.__fill_qsub_file__(configs)
    clean_bcbio.__launch__(configs['system'])
    pipeline.clean_bcbio_key = clean_bcbio.key
    return clean_bcbio

def things_to_do_if_zcat_complete(configs,mockdb,pipeline,zcat):
    zcat.__finish__()
    bcbio = begin_bcbio(configs,mockdb,pipeline,{"zcat": zcat})
    return 1

def things_to_do_if_bcbio_complete(configs,mockdb,pipeline,bcbio):
    store_stats_in_db(bcbio)
    sample = mockdb['Sample'].__get__(configs['system'],pipeline.sample_key)
    clean_bcbio = mockdb['CleanBcbio'].__new__(configs['system'],sample=sample,input_dir=bcbio.output_dir,output_dir=pipeline.output_dir,process_name='clean')
    clean_bcbio.__fill_qsub_file__(configs)
    clean_bcbio.__launch__(configs['system'])
    pipeline.cleaning_key = clean_bcbio.key
    if configs['pipeline'].has_option('Common_directories','output_subdir'):
      output_subdir = configs['pipeline'].get('Common_directories','output_subdir')
    else:
      output_subdir = 'ngv3'
    cp_input_dir = os.path.join(bcbio.upload_dir,bcbio.description)
    cp_result_back = mockdb['CpResultBack'].__new__(configs['system'],sample=sample,pipeline_config=configs["pipeline"],prev_step=bcbio,pipeline=pipeline)
    cp_result_back.__fill_qsub_file__(configs)
    cp_result_back.__launch__(configs['system'])
    pipeline.cp_key = cp_result_back.key
    bcbio.__finish__()
    return 1

def things_to_do_if_snp_stats_complete(configs,mockdb,pipeline,snp_stats):
    store_snp_stats_in_db(snp_stats)
    snp_stats.__finish__()
    if snp_stats.search_key is None:
        return 1
    search = mockdb['ConcordanceSearch'].__get__(configs['system'],snp_stats.search_key)
    store_search_stats_in_db(search)
    search.state = "Complete"
    return 1

def things_to_do_if_summary_stats_complete(configs,mockdb,pipeline,summary_stats):
    summary_stats.__finish__()
    return 1

def things_to_do_if_clean_bcbio_complete(mockdb,pipeline,clean_bcbio):
    """This differs because finishing the pipeline should be outside of this function.  Thus it is not dependent onf storage_devices"""
    clean_bcbio.__finish__()
    return 1


def things_to_do_if_bcbio_cleaning_complete(mockdb,pipeline,clean_bcbio,storage_device=None,*args,**kwargs):
    if storage_devices is None:
        pipeline.__finish__()
    else:
        pipeline.__finish__(storage_device=storage_devices[pipeline.running_location])
    clean_bcbio.__finish__()
    return 1

def things_to_do_if_starting_pipeline(configs,mockdb,pipeline):
    sample = mockdb['Sample'].__get__(configs['system'],pipeline.sample_key)
    if configs["pipeline"].has_option("Pipeline","steps"): #New interface for allowing external definition of linear pipelines
        step_order, step_objects = pipeline.__steps_to_objects__(configs["system"],configs["pipeline"],mockdb)
        first_step = step_order[0]
        if first_step == "zcat_multiple":
            step_objects = begin_next_step(configs,mockdb,pipeline,step_objects,first_step,None)
            pipeline.state = 'Running'
            return 1
        if first_step == "cat":
            step_objects = begin_next_step(configs,mockdb,pipeline,step_objects,first_step,None)
            pipeline.state = 'Running'
            return 1
    section_header = pipeline.running_location + '_directories'
    base_output_dir = configs['pipeline'].get(section_header,'working_directory')
    try:
        project_out = re.sub('_','-',pipeline.project)
        if re.search("[0-9]",project_out[0:1]):
            project_out = "Project-" + project_out
        date=datetime.date.today().strftime("%Y%m%d")
        output_dir = os.path.join(base_output_dir,project_out + "_" + sample.key + '_' + str(date))
        zcat = mockdb['Zcat'].__new__(configs['system'],sample=sample,input_dir=pipeline.input_dir,base_output_dir=base_output_dir,output_dir=output_dir,date=date)
    except AttributeError:
        zcat = mockdb['Zcat'].__new__(configs['system'],sample=sample,input_dir=pipeline.input_dir,base_output_dir=base_output_dir)
    zcat.__fill_qsub_file__(configs)
    pipeline.zcat_key = zcat.key
    zcat.__launch__(configs['system'])
    pipeline.state = 'Running'
    return 1

def things_to_do_if_initializing_pipeline_with_input_directory(configs,storage_devices,mockdb,source_dir,pipeline_name=None,base_output_dir=None):
    sample_dirs = list_sample_dirs(source_dir)
    target_config = ConfigParser.ConfigParser()
    target_config.read(configs["system"].get("Filenames","target_config"))
    for sample in sample_dirs:
        running_location = identify_running_location_with_most_currently_available(configs,storage_devices)
        parsed = parse_sample_sheet(configs['system'],mockdb,sample_dirs[sample][0])
        if base_output_dir is None:
            base_output_dir = configs['pipeline'].get('Common_directories','archive_directory')
        automation_parameters_config = MyConfigParser()
        automation_parameters_config.read(configs["system"].get("Filenames","automation_config"))
        description_pieces = parsed['description'].split('-')
        pipeline_key = description_pieces[-1]
        pipeline_name_for_sample = autmation_parameters_config.safe_get("Pipeline",pipeline_key)
        if not pipeline_name_for_sample is pipeline_name:
            continue
        mockdb[pipeline_name].__new__(configs['system'],input_dir=sample_dirs[sample][0],pipeline_config=configs["pipeline"],project=parsed['project_name'],**parsed)
        flowcell_dict = mockdb['SequencingRun'].__attribute_value_to_object_dict__('flowcell_key')
        flowcell_dict = mockdb['SequencingRun'].__attribute_value_to_object_dict__('flowcell_key')
        if parsed['flowcell'].key in flowcell_dict:
            seq_run = flowcell_dict[parsed['flowcell'].key]
            pass
        else:
            try:
                base_dir = get_sequencing_run_base_dir(source_dir)
                [date,machine_key,run_number,side,flowcell_key] = parse_sequencing_run_dir(base_dir)
                machine = mockdb['HiSeqMachine'].__get__(configs['system'],machine_key)
                run_type = determine_run_type(base_dir)
                seq_run = mockdb['SequencingRun'].__new__(configs['system'],flowcell,machine,date,run_number,output_dir=base_dir,side=side,run_type=run_type)
                fill_demultiplex_stats(configs['system'],mockdb,seq_run.output_dir,flowcell,machine)
            except:
                pass
    return 1

def things_to_do_if_snps_called(configs,mockdb,pipeline,bcbio):
    sample = mockdb['Sample'].__get__(configs['system'],bcbio.sample_key)
    snp_stats = mockdb['SnpStats'].__new__(configs['system'],sample=sample,bcbio=bcbio)
    snp_stats.__fill_qsub_file__(configs)
    pipeline.snp_stats_key = snp_stats.key
    snp_stats.__launch__(configs['system'])
    return 1

def things_to_do_for_reports_object(configs,mockdb,object):
    object.__generate_reports__(configs,mockdb)
    object.__send_reports__(configs['pipeline'],mockdb)
