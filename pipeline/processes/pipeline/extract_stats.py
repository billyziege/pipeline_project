import argparse
import os
import re
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
    return_vals = grab_project_summary_stats(summary_file)
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

if __name__ == '__main__':
    #Handle arguments
    parser = argparse.ArgumentParser(description='Puts the project-summary.csv info into the bcbio database')
    parser.add_argument('sample_key', type=str, help='The sample name from which the bcbio process is keyed')
    parser.add_argument('-s','--summary_file', dest='summary_file', type=str, default=None, help='The path to the summary file.  The default is the output_dir of the bcbio process followed project-summary.csv')    
    args = parser.parse_args()
    config = ConfigParser.ConfigParser()
    config.read('/mnt/iscsi_space/zerbeb/pipeline_project/pipeline/config/qc.cfg')
    mockdb = initiate_mockdb(config)
    sample_bcbio_dict = mockdb['Bcbio'].__attribute_value_to_object_dict__('sample_key')
    bcbio = sample_bcbio_dict[args.sample_key][0]
    store_stats_in_db(bcbio,summary_file=args.summary_file)
    save_mockdb(config,mockdb)
