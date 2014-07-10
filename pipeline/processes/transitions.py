import os
import sys
import re
import datetime
from processes.parsing import parse_sample_sheet
from processes.hiseq.scripts import list_monitoring_dirs, list_sample_dirs
from processes.hiseq.sequencing_run import determine_run_type
from manage_storage.scripts import identify_running_location_with_most_currently_available
from manage_storage.disk_queries import disk_usage
from demultiplex_stats.fill_demultiplex_stats import fill_demultiplex_stats
from processes.snp_stats.extract_stats import store_snp_stats_in_db, store_search_stats_in_db
from processes.pipeline.extract_stats import store_stats_in_db
from sge_email.scripts import send_email

def begin_next_step(configs,mockdb,pipeline,step_objects,next_step,prev_step):
    """The begin functions are to be used with linear pipelines"""
    sys.stderr.write("Beginning "+next_step+" for "+pipeline.sample_key+"\n")
    step_objects[next_step] = globals()["begin_"+next_step](configs,mockdb,pipeline,step_objects,prev_step=prev_step)
    return step_objects

def begin_bcbio(configs,mockdb,pipeline,step_objects,**kwargs):
    zcat = step_objects["zcat"] #Some of the parameters of the bcbio are dependent on the parameters in the zcat object.
    sample = mockdb['Sample'].__get__(configs['system'],pipeline.sample_key)
    flowcell = mockdb['Flowcell'].__get__(configs['system'],pipeline.flowcell_key)
    section_header = pipeline.running_location + '_directories'
    base_output_dir = configs['system'].get(section_header,'bcbio_output')
    bcbio = mockdb['Bcbio'].__new__(configs['system'],sample=sample,flowcell=flowcell,base_output_dir=base_output_dir,r1_path=zcat.r1_path,r2_path=zcat.r2_path,upload_dir=pipeline.output_dir,description=pipeline.description,output_dir=zcat.output_dir)
    pipeline.bcbio_key = bcbio.key
    bcbio.__fill_all_templates__(configs)
    bcbio.__launch__(configs['system'])
    return bcbio

def begin_summary_stats(configs,mockdb,pipeline,step_objects,**kwargs):
    sample = mockdb['Sample'].__get__(configs['system'],pipeline.sample_key)
    summary_stats = mockdb['SummaryStats'].__new__(configs['system'],sample=sample,bcbio=step_objects["bcbio"],capture_target_bed=configs["pipeline"].get("References","capture_target_bed"))
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

def begin_cp_result_back(configs,mockdb,pipeline,step_objects,**kwargs):
    sample = mockdb['Sample'].__get__(configs['system'],pipeline.sample_key)
    if configs['pipeline'].has_option('Common_directories','output_subdir'):
      output_subdir = configs['pipeline'].get('Common_directories','output_subdir')
    else:
      output_subdir = 'ngv3'
    cp_input_dir = os.path.join(pipeline.output_dir,"results")
    cp_result_back = mockdb['CpResultBack'].__new__(configs['system'],sample=sample,input_dir=cp_input_dir,output_dir=pipeline.output_dir,base_output_dir=pipeline.input_dir,output_subdir=output_subdir,process_name='cp')
    cp_result_back.__fill_qsub_file__(configs)
    cp_result_back.__launch__(configs['system'])
    pipeline.cp_result_back_key = cp_result_back.key
    return cp_result_back

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
    cp_result_back = mockdb['CpResultBack'].__new__(configs['system'],sample=sample,bcbio=bcbio,input_dir=cp_input_dir,output_dir=bcbio.upload_dir,base_output_dir=pipeline.input_dir,output_subdir=output_subdir,process_name='cp')
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


def things_to_do_if_bcbio_cleaning_complete(storage_devices,mockdb,pipeline,clean_bcbio):
    pipeline.__finish__(storage_device=storage_devices[pipeline.running_location])
    clean_bcbio.__finish__()
    return 1

def things_to_do_if_starting_pipeline(configs,mockdb,pipeline):
    sample = mockdb['Sample'].__get__(configs['system'],pipeline.sample_key)
    section_header = pipeline.running_location + '_directories'
    base_output_dir = configs['system'].get(section_header,'bcbio_output')
    try:
        project_out = re.sub('_','-',pipeline.project)
        if re.search("[0-9]",project_out[0:1]):
            project_out = "Project-" + project_out
        date=datetime.date.today().strftime("%Y%m%d")
        output_dir = os.path.join(base_output_dir,project_out + "_" + sample.key + '_' + str(date))
        zcat = mockdb['Zcat'].__new__(configs['system'],sample=sample,input_dir=pipeline.input_dir,base_output_dir=base_output_dir,output_dir=output_dir,date=date)
    except AttributeError:
      zcat = mockdb['Zcat'].__new__(configs['system'],sample=sample,input_dir=pipeline.input_dir,base_output_dir=base_output_dir)
    zcat.__fill_qsub_file__(configs['system'])
    pipeline.zcat_key = zcat.key
    zcat.__launch__(configs['system'])
    pipeline.state = 'Running'
    return 1

def things_to_do_if_sequencing_run_is_complete(configs,storage_devices,mockdb,seq_run,pipeline_name):
    flowcell = mockdb['Flowcell'].__get__(configs['system'],seq_run.flowcell_key)
    machine = mockdb['HiSeqMachine'].__get__(configs['system'],seq_run.machine_key)
    fill_demultiplex_stats(configs['system'],mockdb,seq_run.output_dir,flowcell,machine)
    sample_dirs = list_sample_dirs(seq_run.output_dir)
    for sample_dir in sample_dirs:
        running_location = identify_running_location_with_most_currently_available(configs,storage_devices)
        parsed = parse_sample_sheet(configs['system'],mockdb,sample_dir)
        if (re.search('MSBP$',parsed['description']) and (pipeline_name == 'QualityControlPipeline')):
            base_output_dir = configs['pipeline'].get('Common_directories','bcbio_upload')
            pipeline = mockdb['QualityControlPipeline'].__new__(configs['system'],input_dir=sample_dir,base_output_dir=base_output_dir,sequencing=seq_run,running_location=running_location,storage_needed=configs['pipeline'].get('Storage','needed'),project=parsed['project_name'],**parsed)
            #backup = mockdb['Backup'].__new__(config,sample=parsed['sample'],input_dir=sample_dir)
            #backup.__fill_qsub_file__(config)
            #backup.__launch__(config,storage_device=storage_devices[backup.location])
            flowcell_fc_report_dict = mockdb['FlowcellStatisticsReports'].__attribute_value_to_object_dict__('flowcell_key')
            try:
                report = flowcell_fc_report_dict[flowcell.key][0]
            except KeyError:
                report = mockdb['FlowcellStatisticsReports'].__new__(configs['system'],flowcell=flowcell,seq_run=seq_run)
            report.__add_pipeline__(pipeline)
        elif (re.search('NGv3$',parsed['description']) and (pipeline_name == 'StandardPipeline')):
            base_output_dir = configs['pipeline'].get('Common_directories','bcbio_upload')
            mockdb['StandardPipeline'].__new__(configs['system'],input_dir=sample_dir,base_output_dir=base_output_dir,running_location=running_location,storage_needed=configs['pipeline'].get('Storage','needed'),project=parsed['project_name'],**parsed)
        elif (re.search('MHC$',parsed['description']) and (pipeline_name == 'MHCPipeline')):
            base_output_dir = configs['pipeline'].get('Common_directories','bcbio_upload')
            mockdb['MHCPipeline'].__new__(configs['system'],input_dir=sample_dir,base_output_dir=base_output_dir,running_location=running_location,storage_needed=configs['pipeline'].get('Storage','needed'),project=parsed['project_name'],**parsed)
        elif (re.search('NGv3plusUTR$',parsed['description']) and (pipeline_name == 'NGv3PlusPipeline')):
            base_output_dir = configs['pipeline'].get('Common_directories','bcbio_upload')
            mockdb['NGv3PlusPipeline'].__new__(configs['system'],input_dir=sample_dir,base_output_dir=base_output_dir,running_location=running_location,storage_needed=configs['pipeline'].get('Storage','needed'),project=parsed['project_name'],**parsed)
    return 1

def things_to_do_if_initializing_pipeline_with_input_directory(configs,storage_devices,mockdb,source_dir,pipeline_name=None,base_output_dir=None):
    sample_dirs = list_sample_dirs(source_dir)
    for sample_dir in sample_dirs:
        running_location = identify_running_location_with_most_currently_available(configs,storage_devices)
        parsed = parse_sample_sheet(configs['system'],mockdb,sample_dir)
        if base_output_dir is None:
           base_output_dir = configs['pipeline'].get('Common_directories','bcbio_upload')
        if (re.search('MSBP$',parsed['description']) and (pipeline_name == 'QualityControlPipeline')):
            pipeline = mockdb['QualityControlPipeline'].__new__(configs['system'],input_dir=sample_dir,base_output_dir=base_output_dir,running_location=running_location,storage_needed=configs['pipeline'].get('Storage','needed'),project=parsed['project_name'],**parsed)
        elif (re.search('NGv3$',parsed['description']) and (pipeline_name == 'StandardPipeline')):
            mockdb['StandardPipeline'].__new__(configs['system'],input_dir=sample_dir,base_output_dir=base_output_dir,running_location=running_location,storage_needed=configs['pipeline'].get('Storage','needed'),project=parsed['project_name'],**parsed)
        elif (re.search('MHC$',parsed['description']) and (pipeline_name == 'MHCPipeline')):
            mockdb['MHCPipeline'].__new__(configs['system'],input_dir=sample_dir,base_output_dir=base_output_dir,running_location=running_location,storage_needed=configs['pipeline'].get('Storage','needed'),project=parsed['project_name'],**parsed)
        elif (re.search('NGv3plusUTR$',parsed['description']) and (pipeline_name == 'NGv3PlusPipeline')):
            mockdb['NGv3PlusPipeline'].__new__(configs['system'],input_dir=sample_dir,base_output_dir=base_output_dir,running_location=running_location,storage_needed=configs['pipeline'].get('Storage','needed'),project=parsed['project_name'],**parsed)
        if ( pipeline_name == 'RD2Pipeline' ):
            mockdb['RD2Pipeline'].__new__(configs['system'],input_dir=sample_dir,base_output_dir=base_output_dir,running_location=running_location,storage_needed=configs['pipeline'].get('Storage','needed'),project=parsed['project_name'],**parsed)
        if ( pipeline_name == 'DevelPipeline' ):
            mockdb['DevelPipeline'].__new__(configs['system'],input_dir=sample_dir,base_output_dir=base_output_dir,running_location=running_location,storage_needed=configs['pipeline'].get('Storage','needed'),project=parsed['project_name'],**parsed)
        if ( pipeline_name == 'BBPipeline' ):
            mockdb['BBPipeline'].__new__(configs['system'],input_dir=sample_dir,base_output_dir=base_output_dir,running_location=running_location,storage_needed=configs['pipeline'].get('Storage','needed'),project=parsed['project_name'],**parsed)
        if ( pipeline_name == 'BBPipeline' ):
            mockdb['BBPipeline'].__new__(configs['system'],input_dir=sample_dir,base_output_dir=base_output_dir,running_location=running_location,storage_needed=configs['pipeline'].get('Storage','needed'),project=parsed['project_name'],**parsed)
        if ( pipeline_name == 'KanePipeline' ):
            mockdb['KanePipeline'].__new__(configs['system'],input_dir=sample_dir,base_output_dir=base_output_dir,running_location=running_location,storage_needed=configs['pipeline'].get('Storage','needed'),project=parsed['project_name'],**parsed)
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
