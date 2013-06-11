import ConfigParser
import sys
import os
import re
import argparse
from mockdb.initiate_mockdb import initiate_mockdb, save_mockdb

def post_pipeline_report(mockdb,sample_list):
    sep = ','
    header = ["Sample_ID","Date","Sequencing_Index","Lane_number","Flowcell_ID","Machine_ID","Number_reads",
              "Self_concordance","Self_concordance_evaluations","Best_concordance(Sample_ID)",
              "Percentage_of_reads_aligned","Percentage_of_read_duplicates","Insert_size",
              "Percentage_of_reads_on_target_bases","Mean_target_coverage",
              "Percentage_of_reads_with_at_least_10x_coverage_targets",
              "Percentage_of_reads_with_zero_coverage_targets","Total_variations","In_dbSNP",
              "Transition/Transversion_(all)","Transition/Transversion_(dbSNP)",
              "Transition/Transversion_(novel)","Heterozygous/Homozygous"
             ]
    print sep.join(header)
    for sample_key in sample_list:
        out_line = prepare_pipeline_report_line(mockdb,sample_key,sep)
        print out_line
    return 1

def prepare_pipeline_report_line(mockdb,sample_key,sep=','):
    #Get the objects
    sample = mockdb['Sample'].objects[sample_key]
    sample_barcode_dict = mockdb['Barcode'].__attribute_value_to_object_dict__('sample_key')
    try:
        barcode = sample_barcode_dict[sample_key][0]
    except KeyError:
        pieces = sample_key.split('-')
        sample_key2 = pieces[0] + '-' + pieces[1] + '_' + pieces[2]
        #sys.stderr.write("%s\n" % sample_key2)
        barcode = sample_barcode_dict[sample_key2][0]
    lane = mockdb['Lane'].objects[barcode.lane_key]
    flowcell = mockdb['Flowcell'].objects[lane.flowcell_key]
    flowcell_seq_run_dict = mockdb['SequencingRun'].__attribute_value_to_object_dict__('flowcell_key')
    seq_run = flowcell_seq_run_dict[flowcell.key][0]
    #Get the process objects
    sample_qcpipeline_dict = mockdb['QualityControlPipeline'].__attribute_value_to_object_dict__('sample_key')
    sample_stdpipeline_dict = mockdb['StandardPipeline'].__attribute_value_to_object_dict__('sample_key')
    try:
        pipeline = sample_qcpipeline_dict[sample_key][0]
    except KeyError:
        pipeline = sample_stdpipeline_dict[sample_key][0]
    zcat = mockdb['Zcat'].objects[pipeline.zcat_key]
    bcbio = mockdb['Bcbio'].objects[pipeline.bcbio_key]
    if pipeline.__class__.__name__ == 'QualityControlPipeline':
        #if pipeline.snp_stats_key is None:
        #snp_stats = mockdb['SnpStats'].__new__(config,sample=sample,bcbio=bcbio)
        #else:
        #sample_snp_stats_dict = mockdb['SnpStats'].__attribute_value_to_object_dict__('sample_key')
        snp_stats = mockdb['SnpStats'].objects[pipeline.snp_stats_key]
        #snp_stats = sample_snp_stats_dict[sample_key][0]
        #pipeline.snp_stats_key = snp_stats.key
        if snp_stats.search_key is None:
            search = None
        else:
            search = mockdb['ConcordanceSearch'].objects[snp_stats.search_key]
    #Insert the data
    line = [sample_key]
    line += [seq_run.date]
    line += [barcode.index]
    line += [lane.number]
    line += [flowcell.key]
    line += [seq_run.machine_key]
    line += [barcode.reads]
    if pipeline.__class__.__name__ == 'QualityControlPipeline':
        line += [snp_stats.percentage_concordance]
        line += [snp_stats.concordance_calls]
        if search is None:
            line += ['NA']
        else:
            line += [str(search.first_concordance) + '(' + search.first_match + ')']
    line += [bcbio.percent_aligned]
    line += [bcbio.percentage_duplicates]
    line += [bcbio.insert_size]
    line += [bcbio.percentage_on_target_bases]
    line += [bcbio.mean_target_coverage]
    line += [bcbio.percentage_with_at_least_10x_coverage]
    line += [bcbio.percentage_0x_coverage]
    line += [bcbio.total_variations]
    line += [bcbio.percentage_in_db_snp]
    line += [bcbio.titv_all]
    line += [bcbio.titv_dbsnp]
    line += [bcbio.titv_novel]
    if pipeline.__class__.__name__ == 'QualityControlPipeline':
        line += [snp_stats.hethom_ratio]
    line2 = []
    for element in line:
        if element is None:
            line2.append('NA')
        else:
            line2.append(element)
    return sep.join([str(element) for element in line2])


if __name__ == "__main__":
    #Handle arguments
    parser = argparse.ArgumentParser(description='Reports the statistics for the provided sample list')
    parser.add_argument('sample_list_file', type=str, help='A file with a list of samples (one per line)')
    args = parser.parse_args()
    config = ConfigParser.ConfigParser()
    config.read('/mnt/iscsi_space/zerbeb/qc_pipeline_project/qc_pipeline/config/qc.cfg')
    mockdb = initiate_mockdb(config)
    with open(args.sample_list_file,"r") as f:
        samples = [line.strip() for line in f]
    post_pipeline_report(mockdb,samples)
    #save_mockdb(config,mockdb)
