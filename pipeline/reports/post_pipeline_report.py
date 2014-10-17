import ConfigParser
import sys
import os
import re
import csv
import argparse
from texttable import Texttable
from mockdb.initiate_mockdb import initiate_mockdb, save_mockdb
from config.scripts import grab_thresholds_from_config

def post_pipeline_report(mockdb,sample_list):
    sep = ','
    header = ["Sample_ID","Date","Sequencing_Index","Lane_number","Flowcell_ID","Machine_ID","Number_reads",
              "Self_concordance","Self_concordance_evaluations","Best_concordance(Sample_ID)",
              "Percentage_of_reads_aligned","Percentage_of_read_duplicates","Insert_size",
              "Percentage_of_reads_on_target_bases","Mean_target_coverage",
              "Percentage_of_reads_with_at_least_10x_coverage_targets",
              #"Percentage_of_reads_with_zero_coverage_targets","Total_variations","In_dbSNP",
              "Percentage_of_reads_with_zero_coverage_targets","In_dbSNP",
              #"Heterozygous/Homozygous","Picard_in_dbSNP",
              "Heterozygous/Homozygous"
             # "Transition/Transversion_(all)","Transition/Transversion_(dbSNP)",
             # "Transition/Transversion_(novel)"
             ]
    print sep.join(header)
    for sample_key in sample_list:
        #try:
        out_line = prepare_pipeline_report_line(mockdb,sample_key,sep)
        print out_line
        #except:
        #    print sample_key + " failed"
    return 1

def prepare_pipeline_report_line(mockdb,sample_key,sep=','):
    #Get the objects
    try:
        sample = mockdb['Sample'].objects[sample_key]
    except KeyError:
        pieces = sample_key.split('-')
        sample_key2 = pieces[0] + '-' + pieces[1] + '_' + pieces[2]
        #sys.stderr.write("%s\n" % sample_key2)
        sample = mockdb['Sample'].objects[sample_key2]
    sample_barcode_dict = mockdb['Barcode'].__attribute_value_to_object_dict__('sample_key')
    try:
      barcode = sample_barcode_dict[sample.key][0]
    except KeyError:
      pieces = sample_key.split('-')
      sample_key2 = pieces[0] + '-' + pieces[1] + '_' + pieces[2]
      barcode = sample_barcode_dict[sample_key2][0]
    lane = mockdb['Lane'].objects[barcode.lane_key]
    flowcell = mockdb['Flowcell'].objects[lane.flowcell_key]
    flowcell_seq_run_dict = mockdb['SequencingRun'].__attribute_value_to_object_dict__('flowcell_key')
    try:
        seq_run = flowcell_seq_run_dict[flowcell.key][0]
    except:
        seq_run = None
    #Get the process objects
    sample_qcpipeline_dict = mockdb['QualityControlPipeline'].__attribute_value_to_object_dict__('sample_key')
    sample_stdpipeline_dict = mockdb['StandardPipeline'].__attribute_value_to_object_dict__('sample_key')
    try:
        pipeline = sample_qcpipeline_dict[sample.key][0]
    except KeyError:
        try:
          pipeline = sample_stdpipeline_dict[sample.key][0]
        except KeyError:
          pieces = sample_key.split('-')
          sample_key2 = pieces[0] + '-' + pieces[1] + '_' + pieces[2]
          pipeline = sample_qcpipeline_dict[sample_key2][0]
    zcat = mockdb['Zcat'].objects[pipeline.zcat_key]
    bcbio = mockdb['Bcbio'].objects[pipeline.bcbio_key]
    if pipeline.__class__.__name__ == 'QualityControlPipeline':
        #if pipeline.snp_stats_key is None:
        #snp_stats = mockdb['SnpStats'].__new__(config,sample=sample,bcbio=bcbio)
        #else:
        #sample_snp_stats_dict = mockdb['SnpStats'].__attribute_value_to_object_dict__('sample_key')
        try:
            snp_stats = mockdb['SnpStats'].objects[pipeline.snp_stats_key]
            if snp_stats.search_key is None:
                search = None
            else:
                search = mockdb['ConcordanceSearch'].objects[snp_stats.search_key]
        except:
            snp_stats = None
            search = None
        #snp_stats = sample_snp_stats_dict[sample_key][0]
        #pipeline.snp_stats_key = snp_stats.key
    #Insert the data
    line = [sample_key]
    if seq_run is None:
        line += [""]
    else:
        line += [seq_run.date]
    line += [barcode.index]
    line += [lane.number]
    line += [flowcell.key]
    if seq_run is None:
        line += [""]
    else:
        line += [seq_run.machine_key]
    if barcode.reads is None:
        line += [bcbio.total_reads]
    else:
        line += [barcode.reads]
    if pipeline.__class__.__name__ == 'QualityControlPipeline':
        if not snp_stats is None:
            line += [snp_stats.percentage_concordance]
            line += [snp_stats.concordance_calls]
        else:
            line +=['NA']
            line +=['NA']
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
    #line += [bcbio.total_variations]
    if pipeline.__class__.__name__ == 'QualityControlPipeline':
        if not snp_stats is None:
            line += [snp_stats.in_dbsnp]
            line += [snp_stats.hethom_ratio]
        else:
            line +=['NA']
            line +=['NA']
    #line += [bcbio.percentage_in_db_snp]
    #line += [bcbio.titv_all]
    #line += [bcbio.titv_dbsnp]
    #line += [bcbio.titv_novel]
    line2 = []
    for element in line:
        if element is None:
            line2.append('NA')
        else:
            line2.append(element)
    return sep.join([str(element) for element in line2])

def pull_outlier_samples(table,statistic,low_threshold=None,high_threshold=None):
    """
    Goes through the output of post_pipeline_report, which has been
    loaded into a table (rows of dictionary), and looks at the specified column.
    If the column value falls out of the provided range, the sample_ID and
    column's value are recorded in a dictionary, which is returned.  
    At least a lower or an upper bound needs to be provided,
    otherwise this function returns an empty dictionary.
    """
    outliers = {}
    for dictionary in table:
        try:
            sample_id = dictionary['Sample_ID']
            value = dictionary[statistic].strip('x')
        except KeyError:
            continue
        if value == 'NA':
            outliers.update({sample_id: value})
            continue
        if not low_threshold is None:
            if float(value) < float(low_threshold):
                outliers.update({sample_id: value})
                continue
        if not high_threshold is None:
            if float(value) > float(high_threshold):
                outliers.update({sample_id: value})
                continue
    return outliers

def pull_five_best_concordance_matches(mockdb,sample_key):
    """
    Places the five top ranked samples from a concordance search
    and their concordance into a tuple.
    """
    best_matches = []
    sample_qcpipeline_dict = mockdb['QualityControlPipeline'].__attribute_value_to_object_dict__('sample_key')
    pipeline = sample_qcpipeline_dict[sample_key][0]
    snp_stats = mockdb['SnpStats'].objects[pipeline.snp_stats_key]
    if snp_stats.search_key is None:
        return best_matches
    search = mockdb['ConcordanceSearch'].objects[snp_stats.search_key]
    best_matches.append([search.first_match,search.first_concordance])
    best_matches.append([search.second_match,search.second_concordance])
    best_matches.append([search.third_match,search.third_concordance])
    best_matches.append([search.fourth_match,search.fourth_concordance])
    best_matches.append([search.fifth_match,search.fifth_concordance])
    return best_matches

def produce_outlier_table(config,mockdb,fname,na_mark='-'):
    """
    Produces a table as a string of samples from the post_pipeline_report
    output file (fname) that have outlier statistics
    on a subset of the statistics that are included in this
    function and which have thresholds in the config file.
    Only the outlier statistics are reported.  If other samples
    have outlier's in a specific statistic but the given sample doesn't,
    this will appear as a na_mark (default '-') in the table.  If no
    samples are found to be outliers, None is returned.
    """
    outliers_dicts = push_outliers_into_dicts(config,fname)
    #Set up the ouput table.
    out_table = Texttable()
    out_table.set_deco(Texttable.HEADER)
    halign = ['l']
    valign = ['m']
    dtype = ['t']
    width = [20]
    header = ['Sample ID']
    all_sample_keys = set([])
    if len(outliers_dicts['Mean depth'].keys()) > 0:
        halign.append('c')
        valign.append('m')
        dtype.append('i')
        width.append(10)
        header.append('Mean depth')
        all_sample_keys.update(set(outliers_dicts['Mean depth'].keys()))
    if len(outliers_dicts['Het/Hom'].keys()) > 0:
        halign.append('c')
        valign.append('m')
        dtype.append('f')
        header.append('Het/Hom')
        width.append(9)
        all_sample_keys.update(set(outliers_dicts['Het/Hom'].keys()))
    if len(outliers_dicts['Concordance'].keys()) > 0:
        halign.append('c')
        valign.append('m')
        dtype.append('f')
        width.append(13)
        header.append('Concordance')
        all_sample_keys.update(set(outliers_dicts['Concordance'].keys()))
        halign.append('c')
        valign.append('m')
        dtype.append('t')
        width.append(30)
        header.append('Best matches (Concordance)')
    if len(outliers_dicts['Percentage\nin dbSNP'].keys()) > 0:
        halign.append('c')
        valign.append('m')
        dtype.append('f')
        width.append(10)
        header.append('Percentage\nin dbSNP')
        all_sample_keys.update(set(outliers_dicts['Percentage\nin dbSNP'].keys()))

    if len(all_sample_keys) < 1:
        return None
    out_table.set_cols_align(halign)
    out_table.set_cols_valign(valign) 
    out_table.set_cols_dtype(dtype)
    out_table.set_cols_width(width)
    out_table.header(header)
    for sample_key in all_sample_keys:
        row = [sample_key]
        for column in header:
            if column == 'Sample ID':
                continue
            if re.search("Best matches",column):
                continue
            try:
                if column == "Concordance":
                    try:
                        row.append("%.2f" % float(outliers_dicts[column][sample_key]))
                    except ValueError:
                        row.append(outliers_dicts[column][sample_key])
                    best_matches = pull_five_best_concordance_matches(mockdb,sample_key)
                    formatted_matches = []
                    for match in best_matches:
                        try:
                            formatted_matches.append(str(match[0]) + " (" + "%.2f" % float(match[1]) + ")")
                        except:
                            formatted_matches.append(str(match[0]) + " (" + str(match[1]) + ")")
                    row.append("\n".join(formatted_matches))
                elif column == "Het/Hom":
                    try:
                    	row.append("%.2f" % float(outliers_dicts[column][sample_key]))
                    except:
                        row.append(outliers_dicts[column][sample_key])
                elif column == "Percentage\nin dbSNP":
                    try:
                        row.append("%.2f" % float(outliers_dicts[column][sample_key]))
                    except:
                        row.append(outliers_dicts[column][sample_key])
                else:
                    row.append(outliers_dicts[column][sample_key])
            except KeyError:
                row.append(na_mark)
                if column == "Concordance":
                    row.append(na_mark)
        out_table.add_row(row)
    return out_table.draw()

def push_outliers_into_dicts(config,fname):
    """
    This produces dictionaries for the outliers and their values for the
    statistics of interest.
    """
    in_table = csv.DictReader(open(fname,'r'),delimiter=',')
    outliers_dicts = {}
    #Identify the outliers
    statistic = 'Mean_target_coverage'
    low, high = grab_thresholds_from_config(config,'Flowcell_reports','mean_depth_thresholds')
    mean_depth_dict = pull_outlier_samples(in_table,statistic,low_threshold=low,high_threshold=high)
    outliers_dicts.update({'Mean depth': mean_depth_dict})
    statistic = 'Heterozygous/Homozygous'
    low, high = grab_thresholds_from_config(config,'Flowcell_reports','hethom_thresholds')
    hethom_dict = pull_outlier_samples(in_table,statistic,low_threshold=low,high_threshold=high)
    outliers_dicts.update({'Het/Hom': hethom_dict})
    statistic = 'Self_concordance'
    low, high = grab_thresholds_from_config(config,'Flowcell_reports','concordance_thresholds')
    concord_dict = pull_outlier_samples(in_table,statistic,low_threshold=low,high_threshold=high)
    outliers_dicts.update({'Concordance': concord_dict})
    statistic = 'In_dbSNP'
    low, high = grab_thresholds_from_config(config,'Flowcell_reports','dbsnp_thresholds')
    dbsnp_dict = pull_outlier_samples(in_table,statistic,low_threshold=low,high_threshold=high)
    outliers_dicts.update({'Percentage\nin dbSNP': dbsnp_dict})
    return outliers_dicts
    

if __name__ == "__main__":
    #Handle arguments
    parser = argparse.ArgumentParser(description='Reports the statistics for the provided sample list')
    parser.add_argument('input', type=str, help='A file with a list of samples (one per line) for the main function.  A report file for the test_outlier_table function')
    parser.add_argument('--test_outlier_table', dest='test_outlier_table', action='store_true', default=False, help='Coops the script to test the outlier table function.')
    args = parser.parse_args()
    system_config = ConfigParser.ConfigParser()
    system_config.read('/home/sequencing/src/pipeline_project/pipeline/config/ihg_system.cfg')
    pipeline_config = ConfigParser.ConfigParser()
    pipeline_config.read('/home/sequencing/src/pipeline_project/pipeline/config/qc_on_ihg.cfg')
    mockdb = initiate_mockdb(system_config)
    if args.test_outlier_table is True:
        print produce_outlier_table(system_config,mockdb,args.input)
    else:
        with open(args.input,"r") as f:
            samples = [line.strip() for line in f]
        post_pipeline_report(mockdb,samples)
    
