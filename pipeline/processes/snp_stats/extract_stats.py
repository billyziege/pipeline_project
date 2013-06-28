import os
import argparse
import re
import ConfigParser
from mockdb.initiate_mockdb import initiate_mockdb, save_mockdb
def grab_concordance_stats(path):
    """
    Grabs the total snps called and the percentage concordance
    from the second line of a self-concordance file
    """
    if not os.path.isfile(path):
        raise Exception("No such file: {0}\n".format(path))
    with open(path,'r') as f:
        f.readline() #First line is header
        columns = f.readline().rstrip().split("\t")
    return columns[1], columns[2] #The total snps where calls were made and the percentage concordance

def grab_search_stats(path):
    """
    Grabs the sample_key and percentage concordance from the
    last five lines of a concordance search file starting
    with the last line, and then going upward.
    """
    if not os.path.isfile(path):
        raise Exception("No such file: {0}\n".format(path))
    with open(path,'r') as f:
        contents = f.read().split("\n") #First line is header
    return_vals = []
    last_five = contents[-6:-1]
    for line in last_five: #Grab last five lines of sorted file.
        columns = line.rstrip().split("\t")
        return_vals.append(columns[0]) #The sample_key
        return_vals.append(columns[3]) #Its percentage concordance
    return return_vals

def grab_hethom_stats(path):
    """
    Grabs the non-reference homozygous calls count, heterzygous calls count,
    total calls count, and the het/hom ratio from the output of the count
    het-hom script.
    """
    if not os.path.isfile(path):
        raise Exception("No such file: {0}\n".format(path))
    with open(path,'r') as f:
        f.readline() #First line is header
        columns = f.readline().rstrip().split("\t")
    total = int(columns[1]) + int(columns[2])
    return columns[1], columns[2], total, columns[3] #The hom, het, total, ratio
        
def store_snp_stats_in_db(snp_stats,directory=None):
    if snp_stats.concordance_path is None:
        snp_stats.concordance_path =  self.sample_key + '.con'
        snp_stats.hethom_path =  self.sample_key + '.hethom'
    if directory is None:
        snp_stats.concordance_calls, snp_stats.percentage_concordance = grab_concordance_stats(snp_stats.concordance_path)
        snp_stats.hom, snp_stats.het, snp_stats.variants_total, snp_stats.hethom_ratio = grab_hethom_stats(snp_stats.hethom_path)
        return 1
    concordance_path = os.path.join(directory,os.path.basename(snp_stats.concordance_path))
    hethom_path = os.path.join(directory,os.path.basename(snp_stats.hethom_path))
    snp_stats.concordance_calls, snp_stats.percentage_concordance = grab_concordance_stats(concordance_path)
    if os.path.isfile(hethom_path):
        snp_stats.hom, snp_stats.het, snp_stats.variants_total, snp_stats.hethom_ratio = grab_hethom_stats(hethom_path)
    else:
        hethom_dir, hethom_name = os.path.split(hethom_path)
        hethom_name = re.sub("_","-",hethom_name)
        hetthom_path = os.path.join(hethom_dir,hethom_name)
        snp_stats.hom, snp_stats.het, snp_stats.variants_total, snp_stats.hethom_ratio = grab_hethom_stats(hethom_path)

def store_search_stats_in_db(concord_search):
    return_vals = grab_search_stats(self.output_path)
    concord_search.first_match = return_vals[-2]
    concord_search.first_concordance = return_vals[-1]
    concord_search.second_match = return_vals[-4]
    concord_search.second_concordance = return_vals[-3]
    concord_search.third_match = return_vals[-6]
    concord_search.third_concordance = return_vals[-5]
    concord_search.fourth_match = return_vals[-8]
    concord_search.fourth_concordance = return_vals[-7]
    concord_search.fifth_match = return_vals[-10]
    concord_search.fifth_concordance = return_vals[-9]
    return 1

if __name__ == '__main__':
    #Handle arguments
    parser = argparse.ArgumentParser(description='Puts the project-summary.csv info into the bcbio database')
    parser.add_argument('sample_key', type=str, help='The sample name from which the snp_stats process is keyed')
    parser.add_argument('-d','--directory', dest='directory', type=str, default=None, help='The path to the concordance and hethom files.  The default is the files stored in the snp_stats process.')    
    args = parser.parse_args()
    config = ConfigParser.ConfigParser()
    config.read('/mnt/iscsi_space/zerbeb/pipeline_project/pipeline/config/qc.cfg')
    mockdb = initiate_mockdb(config)
    sample_snp_stats_dict = mockdb['SnpStats'].__attribute_value_to_object_dict__('sample_key')
    try:
        snp_stats = sample_snp_stats_dict[args.sample_key][0]
    except KeyError:
        pieces = args.sample_key.split('-')
        sample_key2 = pieces[0] + '-' + pieces[1] + '_' + pieces[2]
        #sys.stderr.write("%s\n" % sample_key2)
        snp_stats = sample_snp_stats_dict[sample_key2][0]
    store_snp_stats_in_db(snp_stats,directory=args.directory)
    save_mockdb(config,mockdb)
