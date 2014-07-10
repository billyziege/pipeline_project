import argparse
import os
import re
import yaml
import ConfigParser
from mockdb.initiate_mockdb import initiate_mockdb, save_mockdb

def grab_project_summary_stats(path):
    """
    Grabs all the information for the first
    sample in the project_summary.csv file, cleans it,
    and returns it as an array.
    """
    if not os.path.isfile(path):
        raise Exception("No such file: {0}\n".format(path))
    with open(path,'r') as f:
        f.readline() #First line is header
        columns = f.readline().rstrip().split(',')
    return_vals = [re.sub('%','',re.sub('x$','',column)) for column in columns]
    return return_vals

def store_stats_in_db(bcbio,summary_file=None):
    if summary_file is None:
        summary_file = os.path.join(bcbio.output_dir,'project-summary.csv')
    if not os.path.isfile(summary_file):
        summary_file = os.path.join(bcbio.output_dir,'project-summary.yaml')
    if not os.path.isfile(summary_file):
        return False
    if os.path.splitext(summary_file)[1] == '.csv':
        return_vals = grab_project_summary_stats(summary_file)
        print return_vals
        bcbio.total_reads = return_vals[1]
        bcbio.percent_aligned = return_vals[2]
        bcbio.percentage_duplicates = return_vals[3]
        bcbio.insert_size = return_vals[4]
        bcbio.percentage_on_target_bases = return_vals[5]
        bcbio.mean_target_coverage = return_vals[6]
        bcbio.percentage_with_at_least_10x_coverage = return_vals[7]
        bcbio.percentage_0x_coverage = return_vals[8]
        bcbio.total_variations = return_vals[9]
        bcbio.percentage_in_db_snp = return_vals[10]
        bcbio.titv_all = return_vals[11]
        bcbio.titv_dbsnp = return_vals[12]
        bcbio.titv_novel = return_vals[13]
        return True
    if os.path.splitext(summary_file)[1] == '.yaml':
        return_vals = grab_project_summary_yaml_stats(summary_file)[0]
        bcbio.gc_content = return_vals['%GC'].strip("'")
        bcbio.total_reads = return_vals['Total reads'] 
        bcbio.percent_aligned = return_vals['Mapped reads pct'].replace('%','')
        bcbio.percentage_duplicates = return_vals['Duplicates pct'].replace('%','')
        bcbio.insert_size = return_vals['Median insert size']
        return True
    return False

def store_total_reads_in_db(bcbio,total_reads_file):
    with open(total_reads_file,'r') as f:
        count = int(f.readline())
    bcbio.total_reads = count/2
    return True


if __name__ == '__main__':
    #Handle arguments
    parser = argparse.ArgumentParser(description='Puts the project-summary .csv or .yaml info into the bcbio database')
    parser.add_argument('sample_key', type=str, help='The sample name from which the bcbio process is keyed')
    parser.add_argument('-s','--summary_file', dest='summary_file', type=str, default=None, help='The path to the summary file.  The default is the output_dir of the bcbio process followed project-summary.csv')    
    parser.add_argument('-t','--total_reads_file', dest='total_reads_file', type=str, default=None, help='The path to the total reads file.')    
    args = parser.parse_args()
    system_config = ConfigParser.ConfigParser()
    system_config.read('/home/sequencing/src/pipeline_project/pipeline/config/ihg_system.cfg')
    pipeline_config = ConfigParser.ConfigParser()
    pipeline_config.read('/home/sequencing/src/pipeline_project/pipeline/config/qc_on_ihg.cfg')
    mockdb = initiate_mockdb(system_config)
    sample_bcbio_dict = mockdb['Bcbio'].__attribute_value_to_object_dict__('sample_key')
    bcbio = sample_bcbio_dict[args.sample_key][0]
    if args.total_reads_file is not None:
        store_total_reads_in_db(bcbio,args.total_reads_file)
    else:
        store_stats_in_db(bcbio,summary_file=args.summary_file)
    save_mockdb(system_config,mockdb)
