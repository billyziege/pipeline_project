import os
import sys
import argparse
import re
import ConfigParser
from mockdb.initiate_mockdb import initiate_mockdb, save_mockdb

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
    try:
        total = int(columns[1]) + int(columns[2])
    except IndexError:
        raise IndexError("The path {0} is malformed.\n".format(path))
    return columns[1], columns[2], total, columns[3] #The hom, het, total, ratio
        
def grab_indbsnp_stats(path):
    """
    Grabs the number of autosomal variants in the dbsnp database, the
    total number of autosomal variants, and the percentage in dbsnp from
    the in dbsnp script.
    """
    if not os.path.isfile(path):
        raise Exception("No such file: {0}\n".format(path))
    with open(path,'r') as f:
        f.readline() #First line is header
        columns = f.readline().rstrip().split("\t")
    return columns[0], columns[1], columns[2] #The indbsnp count, autosomal total, percentage

def grab_summary_stats(path):
    """
    Once the csv summary stats file is produced, upload that data.
    """
    if not os.path.isfile(path):
        raise Exception("No such file: {0}\n".format(path))
    with open(path,'r') as f:
        header = f.readline().rstrip().split(",") #First line is header
        columns = f.readline().rstrip().split(",")
    return dict(zip(header,columns))

def store_summary_stats_in_db(summary_stats):
    ss_dict = grab_summary_stats(summary_stats.summary_stats_path)
    summary_stats.hethom_ratio = ss_dict["Het/Hom"]
    summary_stats.total_reads = ss_dict["Total reads"]
    summary_stats.percent_aligned = ss_dict["Reads aligned"]
    summary_stats.percentage_duplicates = ss_dict["Read duplicates"]
    summary_stats.insert_size = ss_dict["Median read insert size"]
    summary_stats.percentage_on_target_bases = ss_dict["On target bases"]
    summary_stats.percentage_near_target_bases = ss_dict["On or near target bases"]
    summary_stats.mean_target_coverage = ss_dict["Mean target coverage"]
    summary_stats.percentage_with_at_least_10x_coverage = ss_dict["Target bases > 10x coverage"]
    summary_stats.percentage_0x_coverage = ss_dict["Target bases with 0x coverage"]
    summary_stats.percentage_in_db_snp = ss_dict["In dbSNP"]
    summary_stats.ts_tv_ratio = ss_dict["Transitions/Transversions"]
    return 1

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

def store_snp_stats_in_db(snp_stats,directory=None,sample_key=None):
    if snp_stats.concordance_path is None:
        snp_stats.concordance_path =  snp_stats.sample_key + '.con'
        snp_stats.hethom_path =  snp_stats.sample_key + '.hethom'
    if snp_stats.indbsnp_path is None:
        snp_stats.indbsnp_path = snp_stats.sample_key + '.indbsnp'
    #else:
    #    snp_stats.indbsnp_path = snp_stats.sample_key + '.indbsnp'
    if directory is None:
        snp_stats.concordance_calls, snp_stats.percentage_concordance = grab_concordance_stats(snp_stats.concordance_path)
        snp_stats.hom, snp_stats.het, snp_stats.variants_total, snp_stats.hethom_ratio = grab_hethom_stats(snp_stats.hethom_path)
        dummy_indbsnp, dummy_tot, snp_stats.in_dbsnp = grab_indbsnp_stats(snp_stats.indbsnp_path)
        return 1
    if not sample_key is None:
        snp_stats.sample_key = sample_key
        concordance_path = os.path.join(directory,sample_key + '.con')
        hethom_path = os.path.join(directory,sample_key + '.hethom')
        indbsnp_path = os.path.join(directory,sample_key + '.indbsnp')
    else:
        concordance_path = os.path.join(directory,os.path.basename(snp_stats.concordance_path))
        hethom_path = os.path.join(directory,os.path.basename(snp_stats.hethom_path))
        indbsnp_path = os.path.join(directory,os.path.basename(snp_stats.indbsnp_path))
    snp_stats.concordance_calls, snp_stats.percentage_concordance = grab_concordance_stats(concordance_path)
    if os.path.isfile(hethom_path):
        snp_stats.hom, snp_stats.het, snp_stats.variants_total, snp_stats.hethom_ratio = grab_hethom_stats(hethom_path)
    else:
        hethom_dir, hethom_name = os.path.split(hethom_path)
        hethom_name = re.sub("_","-",hethom_name)
        hetthom_path = os.path.join(hethom_dir,hethom_name)
        snp_stats.hom, snp_stats.het, snp_stats.variants_total, snp_stats.hethom_ratio = grab_hethom_stats(hethom_path)
    if os.path.isfile(indbsnp_path):
        dummy_indbsnp, dummy_tot, snp_stats.in_dbsnp = grab_indbsnp_stats(indbsnp_path)
    else:
        indnsnp_dir, indbsnp_name = os.path.split(indbsnp_path)
        indbsnp_name = re.sub("_","-",indbsnp_name)
        indbsnp_path = os.path.join(indbsnp_dir,indbsnp_name)
        dummy_indbsnp, dummy_tot, snp_stats.in_dbsnp = grab_indbsnp_stats(indbsnp_path)
    return 1

def store_search_stats_in_db(concord_search,directory=None,sample_key=None):
    if directory is None:
    	return_vals = grab_search_stats(concord_search.output_path)
    else:
        if sample_key is None:
            out_name = os.path.basename(concord_search.output_path)
        else:
            out_name = sample_key + '_all.con'
        output_path = os.path.join(directory,out_name)
        return_vals = grab_search_stats(output_path)
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
    parser.add_argument('--search', dest='search', action='store_true', help='This extracts and stores the search statistics instead of the snp_stats statistics.')    
    args = parser.parse_args()
    system_config = ConfigParser.ConfigParser()
    system_config.read('/home/sequencing/src/pipeline_project/pipeline/config/ihg_system.cfg')
    pipeline_config = ConfigParser.ConfigParser()
    pipeline_config.read('/home/sequencing/src/pipeline_project/pipeline/config/qc_on_ihg.cfg')
    mockdb = initiate_mockdb(system_config)
    sample_snp_stats_dict = mockdb['SnpStats'].__attribute_value_to_object_dict__('sample_key')
    try:
        snp_stats = sample_snp_stats_dict[args.sample_key][0]
        print snp_stats.sample_key + "," + str(snp_stats.key)
    except KeyError:
        pieces = args.sample_key.split('-')
        sample_key2 = pieces[0] + '-' + pieces[1] + '_' + pieces[2]
        #sys.stderr.write("%s\n" % sample_key2)
        snp_stats = sample_snp_stats_dict[sample_key2][0]
    if args.search:
        concord_search = mockdb['ConcordanceSearch'].objects[snp_stats.search_key]
        store_search_stats_in_db(concord_search,directory=args.directory,sample_key=args.sample_key)
    else:
        store_snp_stats_in_db(snp_stats,directory=args.directory,sample_key=args.sample_key)
    save_mockdb(system_config,mockdb)
