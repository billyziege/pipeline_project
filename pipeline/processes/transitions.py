import sys
import re
from processes.parsing import parse_sample_sheet
from processes.hiseq.scripts import list_sample_dirs
from manage_storage.scripts import identify_running_location_with_most_currently_available
from demultiplex_stats.fill_demultiplex_stats import fill_demultiplex_stats

def things_to_do_if_zcat_complete(config,mockdb,pipeline,zcat):
    zcat.__finish__()
    sample = mockdb['Sample'].__get__(config,pipeline.sample_key)
    flowcell = mockdb['Flowcell'].__get__(config,pipeline.flowcell_key)
    section_header = pipeline.running_location + '_directories'
    base_output_dir = config.get(section_header,'bcbio_output')
    bcbio = mockdb['Bcbio'].__new__(config,sample=sample,flowcell=flowcell,base_output_dir=base_output_dir,r1_path=zcat.r1_path,r2_path=zcat.r2_path,upload_dir=pipeline.output_dir,description=pipeline.description)
    pipeline.bcbio_key = bcbio.key
    bcbio.__fill_all_templates__(config)
    bcbio.__launch__(config)
    return 1

def things_to_do_if_bcbio_complete(config,mockdb,pipeline,bcbio):
    bcbio.__finish__()
    sample = mockdb['Sample'].__get__(config,pipeline.sample_key)
    clean_bcbio = mockdb['CleanBcbio'].__new__(config,sample=sample,input_dir=bcbio.output_dir,output_dir=pipeline.output_dir,process_name='clean')
    clean_bcbio.__fill_qsub_file__(config)
    clean_bcbio.__launch__(config)
    pipeline.cleaning_key = clean_bcbio.key
    return 1

def things_to_do_if_bcbio_cleaning_complete(storage_devices,mockdb,pipeline,clean_bcbio):
    clean_bcbio.__finish__()
    pipeline.__finish__(storage_device=storage_devices[pipeline.running_location])
    return 1

def things_to_do_if_bcbio_cleaning_complete(storage_devices,mockdb,pipeline,clean_bcbio):
    clean_bcbio.__finish__()
    pipeline.__finish__(storage_device=storage_devices[pipeline.running_location])
    return 1

def things_to_do_if_starting_pipeline(config,mockdb,pipeline):
    sample = mockdb['Sample'].__get__(config,pipeline.sample_key)
    base_output_dir = config.get(pipeline.running_location+'_directories','bcbio_output')
    zcat = mockdb['Zcat'].__new__(config,sample=sample,input_dir=pipeline.input_dir,base_output_dir=base_output_dir)
    zcat.__fill_qsub_file__(config)
    pipeline.zcat_key = zcat.key
    zcat.__launch__(config)
    pipeline.state = 'Running'
    return 1

def things_to_do_if_sequencing_run_is_complete(config,storage_devices,mockdb,seq_run):
    flowcell = mockdb['Flowcell'].__get__(config,seq_run.flowcell_key)
    machine = mockdb['HiSeqMachine'].__get__(config,seq_run.machine_key)
    fill_demultiplex_stats(config,mockdb,seq_run.output_dir,flowcell,machine)
    sample_dirs = list_sample_dirs(seq_run.output_dir)
    base_output_dir = config.get('Common_directories','bcbio_upload')
    seq_run.__finish__()
    for sample_dir in sample_dirs:
        running_location = identify_running_location_with_most_currently_available(config,storage_devices)
        parsed = parse_sample_sheet(config,mockdb,sample_dir)
        if re.search('MSBP',parsed['recipe']):
            mockdb['QualityControlPipeline'].__new__(config,input_dir=sample_dir,base_output_dir=base_output_dir,sequencing=seq_run,running_location=running_location,**parsed)
            backup = mockdb['Backup'].__new__(config,sample=parsed['sample'],input_dir=sample_dir)
            backup.__fill_qsub_file__(config)
            backup.__launch__(config,storage_device=storage_devices[backup.location])
    return 1

def things_to_do_if_initializing_pipeline_with_input_directory(config,storage_devices,mockdb,source_dir,base_output_dir=None):
    if base_output_dir == None:
        base_output_dir = config.get('Common_directories','bcbio_upload')
    sample_dirs = list_sample_dirs(source_dir)
    for sample_dir in sample_dirs:
        running_location = identify_running_location_with_most_currently_available(config,storage_devices)
        parsed = parse_sample_sheet(config,mockdb,sample_dir)
        if re.search('MSBP',parsed['recipe']):
            mockdb['QualityControlPipeline'].__new__(config,input_dir=sample_dir,base_output_dir=base_output_dir,running_location=running_location,**parsed)
        else:
            mockdb['StandardPipeline'].__new__(config,input_dir=sample_dir,base_output_dir=base_output_dir,running_location=running_location,**parsed)
    flowcell_dict = mockdb['SequencingRun'].__attribute_value_to_object_dict__('flowcell_key')
    if parsed['flowcell'].key in flowcell_dict:
        seq_run = flowcell_dict[parsed['flowcell'].key]
        pass
    else:
        try:
            base_dir = get_sequencing_run_base_dir(source_dir)
            [date,machine_key,run_number,side,flowcell_key] = parse_sequencing_run_dir(base_dir)
            machine = mockdb['HiSeqMachine'].__get__(config,machine_key)
            seq_run = mockdb['SequencingRun'].__new__(config,flowcell,machine,date,run_number,output_dir=base_dir,side=side)
            fill_demultiplex_stats(config,mockdb,seq_run.output_dir,flowcell,machine)
        except:
            pass
    return 1

def things_to_do_if_snps_called(config,mockdb,pipeline,bcbio):
    sample = mockdb['Sample'].__get__(config,bcbio.sample_key)
    snp_stats = mockdb['SnpStats'].__new__(config,sample=sample,bcbio=bcbio)
    snp_stats.__fill_qsub_file__(config)
    pipeline.snp_stats_key = snp_stats.key
    snp_stats.__launch__(config)
    return 1
