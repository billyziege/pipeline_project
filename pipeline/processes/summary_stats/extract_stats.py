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
