import os
import re
import sys
import subprocess
import argparse

def read_single_header_table_stats(in_file,glue = "\t"):
    with open(in_file,'r') as f:
        lines = []
        for line in f:
            line = line.rstrip();
            if line.startswith('#'):
                continue
            if line == "":
                continue
            lines.append(line)
        header = lines[0].split(glue)
        content = lines[1].split(glue)
        dictionary = dict(zip(header,content))
    return dictionary

def read_yaml_stats(in_file):
    dictionary = {}
    with open(in_file,'r') as f:
        for line in f:
            line = line.rstrip();
            if line.startswith('#'):
                continue
            content = line.split(":")
            if (len(content) < 2):
                continue
            line_dict = {}
            values = content[1].split("\t")
            line_dict["raw"] = values[0].replace(" ","")
            if (len(values) == 2):
                line_dict["percentage"] = values[1].replace('(','').replace(')','').replace(" ","")
            dictionary.update({content[0]: line_dict})
    return dictionary

def format_stat(dictionary,key,sub_key=None,round_digits=None,units="",factor=None):
    if sub_key == None:
        stat = dictionary[key]
    else: 
        stat = dictionary[key][sub_key]
    if factor != None:
        stat = float(stat) * factor
    if round_digits != None:
        stat = str(stat).replace('%','')
        stat = round(float(stat),round_digits)
        if round_digits == 0:
            stat = '%d' % stat
    return str(stat) + units

if __name__ == '__main__':
    #Handle arguments
    parser = argparse.ArgumentParser(description='Extract stats we want to report and print them in a table')
    parser.add_argument('sample', type=str, help='The name of the sample you want to appear in the sample column')
    parser.add_argument('hs_metrics_file', type=str, help='The stats_file created by Picard tools CalculateHsMetrics')
    parser.add_argument('bamtools_stats_file', type=str, help='The stats_file produced by the bamtools package in the pipeline')
    parser.add_argument('vcf_stats_file', type=str, help='The stats_file produced by vcftools')
    parser.add_argument('hethom_stats_file', type=str, help='The stats_file produced from my het/hom calculator')
    parser.add_argument('indbsnp_stats_file', type=str, help='The stats_file produced from my indSNP calculator')
    args = parser.parse_args()
    bamtools_stats= read_yaml_stats(args.bamtools_stats_file)
    hs_metrics = read_single_header_table_stats(args.hs_metrics_file)
    on_target_bases = hs_metrics["ON_TARGET_BASES"]
    total_bases = hs_metrics["PF_UQ_BASES_ALIGNED"]
    hs_metrics["PCT_ON_TARGET"] = float(on_target_bases)/float(total_bases)
    vcf_stats= read_yaml_stats(args.vcf_stats_file)
    hethom = read_single_header_table_stats(args.hethom_stats_file)
    indbsnp = read_single_header_table_stats(args.indbsnp_stats_file)
    output_header = ["Sample","Total reads","Reads aligned","Read duplicates","Median read insert size","On target bases","On or near target bases","Mean target coverage","Target bases > 10x coverage","Target bases with 0x coverage","Total SNPs","Transitions/Transversions","Het/Hom","In dbSNP"]
    output_content = [str(args.sample)]
    output_content.append(format_stat(bamtools_stats,"Total reads","raw"))
    output_content.append(format_stat(bamtools_stats,"Mapped reads","percentage",1,'%'))
    output_content.append(format_stat(bamtools_stats,"Duplicates","percentage",1,'%'))
    output_content.append(format_stat(bamtools_stats,"Median insert size (absolute value)","raw",0,' bp'))
    output_content.append(format_stat(hs_metrics,"PCT_ON_TARGET",None,1,'%',100))
    output_content.append(format_stat(hs_metrics,"PCT_SELECTED_BASES",None,1,'%',100))
    output_content.append(format_stat(hs_metrics,"MEAN_TARGET_COVERAGE",None,0,'x'))
    output_content.append(format_stat(hs_metrics,"PCT_TARGET_BASES_10X",None,1,'%',100))
    output_content.append(format_stat(hs_metrics,"ZERO_CVG_TARGETS_PCT",None,1,'%',100))
    output_content.append(format_stat(vcf_stats,"snps","percentage"))
    output_content.append(format_stat(vcf_stats,"ts/tv ratio","percentage",2))
    output_content.append(format_stat(hethom,"ratio",None,2))
    output_content.append(format_stat(indbsnp,"percentage",None,1,'%'))
    print ','.join(output_header)
    print ','.join(output_content)
