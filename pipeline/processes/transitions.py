import os
import ConfigParser
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

def begin_next_step(configs,mockdb,pipeline,step_objects,next_step_key,prev_step_key):
    """The begin functions are to be used with linear pipelines"""
    sys.stderr.write("Beginning "+next_step_key+" for "+pipeline.sample_key+"\n")
    step_objects[next_step_key] = globals()["begin_"+next_step_key](configs,mockdb,pipeline,step_objects,prev_step_key=prev_step_key)
    return step_objects

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

def begin_bwa_aln(configs,mockdb,pipeline,step_objects,prev_step_key,**kwargs):
    sample = mockdb['Sample'].__get__(configs['system'],pipeline.sample_key)
    ref_fa = configs['pipeline'].get('References','genome_fasta')
    bwa_aln = mockdb['BwaAln'].__new__(configs['system'],sample=sample,prev_step=step_objects[prev_step_key],ref_fa=ref_fa,**kwargs)
    pipeline.bwa_aln_key = bwa_aln.key
    bwa_aln.__fill_qsub_file__(configs)
    bwa_aln.__launch__(configs['system'])
    return bwa_aln

def begin_bwa_sampe(configs,mockdb,pipeline,step_objects,prev_step_key,**kwargs):
    sample = mockdb['Sample'].__get__(configs['system'],pipeline.sample_key)
    bwa_sampe = mockdb['BwaSampe'].__new__(configs['system'],sample=sample,prev_step=step_objects[prev_step_key],multi_fastq_file=step_objects["zcat_multiple"].multi_fastq_file,ref_fa=step_objects["bwa_aln"].ref_fa,project=pipeline.project,**kwargs)
    pipeline.bwa_sampe_key = bwa_sampe.key
    bwa_sampe.__fill_qsub_file__(configs)
    bwa_sampe.__launch__(configs['system'])
    return bwa_sampe

def begin_fast_q_c(configs,mockdb,pipeline,step_objects,prev_step_key,**kwargs):
    sample = mockdb['Sample'].__get__(configs['system'],pipeline.sample_key)
    fastqc = mockdb['FastQC'].__new__(configs['system'],sample=sample,input_dir=step_objects[prev_step_key].output_dir,output_dir=step_objects[prev_step_key].output_dir,**kwargs)
    pipeline.fast_q_c_key = fastqc.key
    fastqc.__fill_qsub_file__(configs)
    fastqc.__launch__(configs['system'])
    return fastqc

def begin_sam_conversion(configs,mockdb,pipeline,step_objects,prev_step_key,**kwargs):
    sample = mockdb['Sample'].__get__(configs['system'],pipeline.sample_key)
    sam_conversion = mockdb['SamConversion'].__new__(configs['system'],sample=sample,prev_step=step_objects[prev_step_key],**kwargs)
    pipeline.sam_conversion_key = sam_conversion.key
    sam_conversion.__fill_qsub_file__(configs)
    sam_conversion.__launch__(configs['system'])
    return sam_conversion

def begin_sort_bam(configs,mockdb,pipeline,step_objects,prev_step_key,**kwargs):
    sample = mockdb['Sample'].__get__(configs['system'],pipeline.sample_key)
    if configs["system"].get("Logging","debug") is "True":
       print "  Sample: " + sample.key 
    sort_bam = mockdb['SortBam'].__new__(configs['system'],sample=sample,prev_step=step_objects[prev_step_key],**kwargs)
    if configs["system"].get("Logging","debug") is "True":
       print "  SortBam: " + str(sort_bam.key) 
    pipeline.sort_bam_key = sort_bam.key
    sort_bam.__fill_qsub_file__(configs)
    sort_bam.__launch__(configs['system'])
    return sort_bam

def begin_merge_bam(configs,mockdb,pipeline,step_objects,prev_step_key,**kwargs):
    sample = mockdb['Sample'].__get__(configs['system'],pipeline.sample_key)
    if configs["system"].get("Logging","debug") is "True":
       print "  Sample: " + sample.key 
    merge_bam = mockdb['MergeBam'].__new__(configs['system'],sample=sample,prev_step=step_objects[prev_step_key],**kwargs)
    if configs["system"].get("Logging","debug") is "True":
       print "  MergeBam: " + str(merge_bam.key) 
    pipeline.merge_bam_key = merge_bam.key
    merge_bam.__fill_qsub_file__(configs)
    merge_bam.__launch__(configs['system'])
    return merge_bam

def begin_mark_duplicates(configs,mockdb,pipeline,step_objects,prev_step_key,**kwargs):
    sample = mockdb['Sample'].__get__(configs['system'],pipeline.sample_key)
    mark_duplicates = mockdb['MarkDuplicates'].__new__(configs['system'],sample=sample,prev_step=step_objects[prev_step_key],**kwargs)
    pipeline.mark_duplicates_key = mark_duplicates.key
    mark_duplicates.__fill_qsub_file__(configs)
    mark_duplicates.__launch__(configs['system'])
    return mark_duplicates

def begin_indel_realignment(configs,mockdb,pipeline,step_objects,prev_step_key,**kwargs):
    sample = mockdb['Sample'].__get__(configs['system'],pipeline.sample_key)
    dbsnp_vcf = configs['pipeline'].get('References','dbsnp_vcf')
    indel_realignment = mockdb['IndelRealignment'].__new__(configs['system'],sample=sample,prev_step=step_objects[prev_step_key],ref_fa=step_objects["bwa_aln"].ref_fa,dbsnp_vcf=dbsnp_vcf,**kwargs)
    pipeline.indel_realignment_key = indel_realignment.key
    indel_realignment.__fill_qsub_file__(configs)
    indel_realignment.__launch__(configs['system'])
    return indel_realignment

def begin_base_recalibration(configs,mockdb,pipeline,step_objects,prev_step_key,**kwargs):
    sample = mockdb['Sample'].__get__(configs['system'],pipeline.sample_key)
    base_recalibration = mockdb['BaseRecalibration'].__new__(configs['system'],sample=sample,prev_step=step_objects[prev_step_key],ref_fa=step_objects["bwa_aln"].ref_fa,dbsnp_vcf=step_objects["indel_realignment"].dbsnp_vcf,**kwargs)
    pipeline.base_recalibration_key = base_recalibration.key
    base_recalibration.__fill_qsub_file__(configs)
    base_recalibration.__launch__(configs['system'])
    return base_recalibration

def begin_unified_genotyper(configs,mockdb,pipeline,step_objects,prev_step_key,**kwargs):
    sample = mockdb['Sample'].__get__(configs['system'],pipeline.sample_key)
    unified_genotyper = mockdb['UnifiedGenotyper'].__new__(configs['system'],sample=sample,prev_step=step_objects[prev_step_key],ref_fa=step_objects["bwa_aln"].ref_fa,dbsnp_vcf=step_objects["indel_realignment"].dbsnp_vcf,**kwargs)
    pipeline.unified_genotyper_key = unified_genotyper.key
    unified_genotyper.__fill_qsub_file__(configs)
    unified_genotyper.__launch__(configs['system'])
    return unified_genotyper

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

def begin_clean(configs,mockdb,pipeline,step_objects,prev_step_key,**kwargs):
    sample = mockdb['Sample'].__get__(configs['system'],pipeline.sample_key)
    clean = mockdb['Clean'].__new__(configs['system'],sample=sample,input_dir=step_objects[prev_step_key].output_dir,output_dir=pipeline.output_dir,process_name='clean')
    clean.__fill_qsub_file__(configs)
    clean.__launch__(configs['system'])
    pipeline.clean_key = clean.key
    return clean

def begin_cp_result_back(configs,mockdb,pipeline,step_objects,prev_step_key,**kwargs):
    sample = mockdb['Sample'].__get__(configs['system'],pipeline.sample_key)
    if configs['pipeline'].has_option('Common_directories','output_subdir'):
        output_subdir = configs['pipeline'].get('Common_directories','output_subdir')
    else:
        output_subdir = 'ngv3'
    if configs['pipeline'].has_option('Common_directories','cp_subdir'):
        if configs['pipeline'].get('Common_directories','cp_subdir') == "None":
            cp_input_dir = pipeline.output_dir
        else:
            cp_input_dir = os.path.join(pipeline.output_dir,configs['pipeline'].get('Common_directories','cp_subdir'))
    else:
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

def things_to_do_if_sequencing_run_is_complete(configs,storage_devices,mockdb,seq_run,pipeline_name):
    flowcell = mockdb['Flowcell'].__get__(configs['system'],seq_run.flowcell_key)
    machine = mockdb['HiSeqMachine'].__get__(configs['system'],seq_run.machine_key)
    if configs["system"].get("Logging","debug") is "True":
        print "  Flowcell " + flowcell.key
        print "  Machine " + machine.key
        print "  Filling stats"
    fill_demultiplex_stats(configs['system'],mockdb,seq_run.output_dir,flowcell,machine)
    if configs["system"].get("Logging","debug") is "True":
        print "  Determining dirs"
    sample_dirs = list_sample_dirs(seq_run.output_dir.split(":"))
    if configs["system"].get("Logging","debug") is "True":
       print "  Samples: " + str(sample_dirs) 
    target_config = ConfigParser.ConfigParser()
    target_config.read(configs["system"].get("Filenames","target_config"))
    for sample in sample_dirs:
        if configs["system"].get("Logging","debug") is "True":
           print "    Processing " + sample
        running_location = identify_running_location_with_most_currently_available(configs,storage_devices)
        parsed = parse_sample_sheet(configs['system'],mockdb,sample_dirs[sample][0])
        if (re.search('MSBP$',parsed['description']) and (pipeline_name == 'QualityControlPipeline')):
            base_output_dir = configs['pipeline'].get('Common_directories','archive_directory')
            pipeline = mockdb['QualityControlPipeline'].__new__(configs['system'],input_dir=sample_dir[sample][0],base_output_dir=base_output_dir,sequencing=seq_run,running_location=running_location,storage_needed=configs['pipeline'].get('Storage','needed'),project=parsed['project_name'],**parsed)
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
            base_output_dir = configs['pipeline'].get('Common_directories','archive_directory')
            mockdb['StandardPipeline'].__new__(configs['system'],input_dir=sample_dirs[sample][0],base_output_dir=base_output_dir,running_location=running_location,storage_needed=configs['pipeline'].get('Storage','needed'),project=parsed['project_name'],**parsed)
        elif (pipeline_name == 'FastQCPipeline'):
            base_output_dir = configs['pipeline'].get('Common_directories','archive_directory')
            mockdb['FastQCPipeline'].__new__(configs['system'],input_dir=sample_dirs[sample][0],base_output_dir=base_output_dir,running_location=running_location,storage_needed=configs['pipeline'].get('Storage','needed'),project=parsed['project_name'],**parsed)
        elif (re.search('MHC$',parsed['description']) and (pipeline_name == 'MHCPipeline')):
            base_output_dir = configs['pipeline'].get('Common_directories','archive_directory')
            mockdb['MHCPipeline'].__new__(configs['system'],input_dir=sample_dirs[sample][0],base_output_dir=base_output_dir,running_location=running_location,storage_needed=configs['pipeline'].get('Storage','needed'),project=parsed['project_name'],**parsed)
        elif (re.search('NGv3plusUTR$',parsed['description']) and (pipeline_name == 'NGv3PlusPipeline')):
            base_output_dir = configs['pipeline'].get('Common_directories','archive_directory')
            pipeline = mockdb['NGv3PlusPipeline'].__new__(configs['system'],input_dir=sample_dirs[sample][0],base_output_dir=base_output_dir,running_location=running_location,storage_needed=configs['pipeline'].get('Storage','needed'),project=parsed['project_name'],capture_target_bed=target_config.get("Pipeline",'NGv3plusUTR'),**parsed)
        elif (re.search('MLEZHX1$',parsed['description']) and (pipeline_name == 'NGv3PlusPipeline')):
            base_output_dir = configs['pipeline'].get('Common_directories','archive_directory')
            pipeline = mockdb['NGv3PlusPipeline'].__new__(configs['system'],input_dir=sample_dirs[sample][0],base_output_dir=base_output_dir,running_location=running_location,storage_needed=configs['pipeline'].get('Storage','needed'),project=parsed['project_name'],capture_target_bed=target_config.get("Pipeline",'MLEZHX1'),**parsed)
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
        if (re.search('MSBP$',parsed['description']) and (pipeline_name == 'QualityControlPipeline')):
            pipeline = mockdb['QualityControlPipeline'].__new__(configs['system'],input_dir=sample_dirs[sample][0],base_output_dir=base_output_dir,running_location=running_location,storage_needed=configs['pipeline'].get('Storage','needed'),project=parsed['project_name'],**parsed)
        elif (re.search('NGv3$',parsed['description']) and (pipeline_name == 'StandardPipeline')):
            mockdb['StandardPipeline'].__new__(configs['system'],input_dir=sample_dirs[sample][0],base_output_dir=base_output_dir,running_location=running_location,storage_needed=configs['pipeline'].get('Storage','needed'),project=parsed['project_name'],**parsed)
        elif (pipeline_name == 'FastQCPipeline'):
            mockdb['FastQCPipeline'].__new__(configs['system'],input_dir=sample_dirs[sample][0],base_output_dir=base_output_dir,running_location=running_location,storage_needed=configs['pipeline'].get('Storage','needed'),project=parsed['project_name'],**parsed)
        elif (re.search('MHC$',parsed['description']) and (pipeline_name == 'MHCPipeline')):
            mockdb['MHCPipeline'].__new__(configs['system'],input_dir=sample_dirs[sample][0],base_output_dir=base_output_dir,running_location=running_location,storage_needed=configs['pipeline'].get('Storage','needed'),project=parsed['project_name'],**parsed)
        elif (re.search('NGv3plusUTR$',parsed['description']) and (pipeline_name == 'NGv3PlusPipeline')):
            mockdb['NGv3PlusPipeline'].__new__(configs['system'],input_dir=sample_dirs[sample][0],base_output_dir=base_output_dir,running_location=running_location,storage_needed=configs['pipeline'].get('Storage','needed'),project=parsed['project_name'],capture_target_bed=target_config.get("Pipeline",'NGv3plusUTR'),**parsed)
        elif (re.search('MLEZHX1$',parsed['description']) and (pipeline_name == 'NGv3PlusPipeline')):
            mockdb['NGv3PlusPipeline'].__new__(configs['system'],input_dir=sample_dirs[sample][0],base_output_dir=base_output_dir,running_location=running_location,storage_needed=configs['pipeline'].get('Storage','needed'),project=parsed['project_name'],capture_target_bed=target_config.get("Pipeline",'MLEZHX1'),**parsed)
        if ( pipeline_name == 'RD2Pipeline' ):
            mockdb['RD2Pipeline'].__new__(configs['system'],input_dir=sample_dirs[sample][0],base_output_dir=base_output_dir,running_location=running_location,storage_needed=configs['pipeline'].get('Storage','needed'),project=parsed['project_name'],**parsed)
        if ( pipeline_name == 'DevelPipeline' ):
            mockdb['DevelPipeline'].__new__(configs['system'],input_dir=sample_dirs[sample][0],base_output_dir=base_output_dir,running_location=running_location,storage_needed=configs['pipeline'].get('Storage','needed'),project=parsed['project_name'],**parsed)
        if ( pipeline_name == 'BBPipeline' ):
            mockdb['BBPipeline'].__new__(configs['system'],input_dir=sample_dirs[sample][0],base_output_dir=base_output_dir,running_location=running_location,storage_needed=configs['pipeline'].get('Storage','needed'),project=parsed['project_name'],**parsed)
        if ( pipeline_name == 'BBPipeline' ):
            mockdb['BBPipeline'].__new__(configs['system'],input_dir=sample_dirs[sample][0],base_output_dir=base_output_dir,running_location=running_location,storage_needed=configs['pipeline'].get('Storage','needed'),project=parsed['project_name'],**parsed)
        if ( pipeline_name == 'KanePipeline' ):
            mockdb['KanePipeline'].__new__(configs['system'],input_dir=sample_dirs[sample][0],base_output_dir=base_output_dir,running_location=running_location,storage_needed=configs['pipeline'].get('Storage','needed'),project=parsed['project_name'],**parsed)
        if pipeline_name == 'TCSPipeline':
            pipeline_steps = configs["pipeline"].get("Pipeline","steps").split(",")
            mockdb['TCSPipeline'].__new__(configs['system'],input_dir=":".join(sample_dirs[sample]),base_output_dir=base_output_dir,running_location=running_location,storage_needed=configs['pipeline'].get('Storage','needed'),project=parsed['project_name'],pipeline_steps=pipeline_steps,**parsed)
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
