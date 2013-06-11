import os
import re
import sys
import subprocess
import argparse
#Dirs storing affy calls
SAMPLE_LIST_DIRS=['/mnt/iscsi_space/zerbeb/affy_samples/msbp','/mnt/iscsi_space/zerbeb/affy_samples/GERA']#,'/mnt/coldstorage/open-transfers/affy_samples/GO']

#Perl scripts
EXTRACT_SAMPLE_SCRIPT='/mnt/iscsi_space/zerbeb/qc_pipeline_project/qc_pipeline/processes/snp_stats/extract_sample_list.pl'
TRANSLATE_AND_FILTER_SCRIPT = '/mnt/iscsi_space/zerbeb/qc_pipeline_project/qc_pipeline/processes/snp_stats/translate_snp_call_while_filtering_from_bed.pl'
VCF_CONVERSION_SCRIPT = '/mnt/iscsi_space/zerbeb/qc_pipeline_project/qc_pipeline/processes/snp_stats/report_snp_vcf_while_filtering_from_bed.pl'

#Files
FILTER_FILE = '/mnt/iscsi_space/zerbeb/data/affy_on_exome.bed'
SNP_LOOKUP_FILE = '/mnt/iscsi_speed/kvale/gcp/combined_GO_annotations_v8.tsv'

def text_to_file(string,fname,directory=os.getcwd()):
    """
    A fast wrapper that writes a string to the file fname in directory.
    """
    out_file = os.path.join(directory,fname)
    with open(out_file,"w") as f:
        f.write(string)
    return out_file

def find_sample_in_lists(sample_key):
    """
    The preprocessed list of samples in the affy call files
    is searched for the sample key.  The first file containing 
    the sample key is returned.
    """
    for directory in SAMPLE_LIST_DIRS:
        for fname in os.listdir(directory):
            if os.path.isfile(os.path.join(directory,fname)):
                if re.search("_samples.txt$",fname):
                    sample_dict = {}
                    with open(os.path.join(directory,fname),"r") as f:
                        samples = f.read().strip().split(',')
                        for sample in samples:
                            short_sample,long_sample = sample.split(":")
                            sample_dict.update({short_sample: long_sample})
                    if sample_key in sample_dict.keys():
                        number = re.sub("_samples.txt$","",fname);
                        return  directory, number, sample_dict[sample_key]
    raise Exception("Sample {0} not in lists.".format(sample_key))

def grab_affy_sample_genotypes_file(sample_key):
    """
    The preprocessed affy genotypes are named by the sample key.
    The function looks in the appropriate directories for the
    file with the genotype calls.
    """
    for directory in SAMPLE_LIST_DIRS:
        fname = os.path.join(directory,sample_key + ".geno")
        if os.path.isfile(fname):
            return fname
    raise Exception("Sample {0} not in lists.".format(sample_key))

def ensure_sample_format(sample_key):
    """
    The files storing affy genotype calls have the format K-NDNA0####_[A-H][0-1][0-9].
    This checks to make sure that the sample key has this format so that it can find the
    appropritate affy file.
    """
    pieces = sample_key.replace('_','-').split('-')
    if len(pieces) != 3:
        raise Exception("Error in sample format.")
    new_sample_key = pieces[0] + "-" + pieces[1] +"_" + pieces[2]
    return new_sample_key

def extract_samples_genotypes(fname,sample_keys,directory=os.getcwd(),script=EXTRACT_SAMPLE_SCRIPT):
    """
    This function calls the perl script that extracts the sample affy calls
    from the appropriate file.
    """
    samples_text = "\n".join(sample_keys)
    out_file = os.path.join(directory,"samples.call")
    samples_fname = text_to_file(samples_text,"samples.ls",directory)
    command = [script,fname,samples_fname]
    proc = subprocess.Popen(command,stdout=subprocess.PIPE)
    proc.wait
    with open(out_file,'w') as f:
        for line in proc.stdout:
            f.write(line)
    return out_file

def translate_samples_genotypes(fname,directory=os.getcwd(),script=TRANSLATE_AND_FILTER_SCRIPT,filter=FILTER_FILE,lookup=SNP_LOOKUP_FILE):
    """
    Affy calls are made in the format 0,1, 2, or ..  This calls the perl script
    that translates these calls to basecall/basecall (basecall = A,C,G, T, or .) 
    for a chr and pos based on the specifications of the SNP and 
    only on the SNPs in the filter file.  The two basecalls in
    basecall/basecall are alphabetized when they are reported 
    (i.e. A/C, A/T, G/T, C/C, etc.)
    """
    out_file = os.path.join(directory,"samples.geno")
    command = [script,fname,filter,lookup]
    proc = subprocess.Popen(command,stdout=subprocess.PIPE)
    proc.wait
    with open(out_file,'w') as f:
        for line in proc.stdout:
            f.write(line)
    return out_file

def convert_variants_form(fname,sample_key,directory=os.getcwd(),script=VCF_CONVERSION_SCRIPT,filter=FILTER_FILE):
    """
    A vcf file is read in and converted to basecall/basecall
    (basecall = A,C,G, T, or .) for a chr and pos only
    for SNPs in the filter file.  The two basecalls in
    basecall/basecall are alphabetized when they are reported 
    (i.e. A/C, A/T, G/T, C/C, etc.)
    """
    out_file = os.path.join(directory,sample_key + "-variants.geno")
    command = [script,fname,filter]
    sys.stderr.write("{0}\n".format(" ".join(command)))
    proc = subprocess.Popen(command,stdout=subprocess.PIPE)
    proc.wait
    with open(out_file,'w') as f:
        for line in proc.stdout:
            f.write(line)
    return out_file

def report_concordance(affy_fname,pipeline_fname):
    """
    This compares two similar files with the format
    "chr pos basecall/basecall" per line on SNPs that
    have information in both files.  The # of same
    calls, total # of calls, and the distribution of errors is
    reported as a string.
    """
    acalls = read_genotype_calls(affy_fname)
    pcalls = read_genotype_calls(pipeline_fname)
    same,total,errors = concordance_of_intersect_calls(acalls,pcalls)

    if total == 0:
        percentage = 0
    else:
        percentage = float(same)*float(100)/float(total)
    
    string = str(same)
    string += "\t" + str(total)
    string += "\t%4f" % percentage
    error_keys = []
    error_values = []
    for key, value in errors.iteritems():
        error_keys.append(str(key))
        error_values.append(str(value))
    string += "\t" + ":".join(error_keys)
    string += "\t" + ":".join(error_values)
    return string

def read_genotype_calls(fname):
    calls = {}
    with open(fname,'r') as f:
       for line in f: 
           columns = line.rstrip().split("\t")
           try:
               calls[columns[0]]
           except:
               calls[columns[0]] = {}
           try:
               calls[columns[0]].update({columns[1]:alphabetize_call(columns[2])})
           except:
               calls[columns[0]].update({columns[1]:alphabetize_call(columns[2])})
    return calls

def alphabetize_call(call):
    alleles = call.split('/')
    alleles.sort()
    return '/'.join([str(a) for a in alleles])

def concordance_of_intersect_calls(affy_calls,pipeline_calls):
    total = 0
    same = 0
    errors = {}
    for chr in affy_calls.keys():
        try:
            pipeline_calls[chr]
        except KeyError:
            continue
        for pos, affy_call in affy_calls[chr].iteritems():
            try:
                pipeline_call = pipeline_calls[chr][pos]
            except KeyError:
                continue
            if re.search('0',affy_call) or re.search('\.',affy_call) or re.search('\-',affy_call):
                continue
            if re.search('0',pipeline_call) or re.search('\.',pipeline_call) or re.search('\-',pipeline_call):
                continue
            total += 1
            if affy_call == pipeline_call:
                same += 1
            else:
                error = affy_call + "-->" + pipeline_call
                try:
                    errors[error] += 1
                except KeyError:
                    errors[error] = 1
    return (same,total,errors)

if __name__ == '__main__':
    #Handle arguments
    parser = argparse.ArgumentParser(description='Report concordance between pipeline vcf and affy calls')
    parser.add_argument('vcf_file', type=str, help='The pipeline vcf file')
    parser.add_argument('-s','--sample', dest='sample', type=str, default=None, help='The sample name to compare needed to find affy calls.  Default is the first 15 characters of the vcf file.')
    parser.add_argument('-o', '--output_file', dest='output_file', type=str, default=None, help='The concordance results will be written to this file.  The default is sample.con.')
    parser.add_argument('-d', '--output_dir', dest='output_dir', type=str, default=None, help='The concordance results will be written to this directory.  The default is the directory of the vcf_file.  If no directory information is found with the vcf file, the output directory is the current working directory.  This is overwritten if output_file has directory information begining with a slash.')
    parser.add_argument('--search_file', dest='search_file', type=str, default=None, help='This finds the concordance of given sample against all of the files listed in file specified by the search file flag.  Without this flag, the program only looks for a single file (corresponding to sample)')
    parser.add_argument('--vcf_conversion_script', dest='vcf_conversion_script', type=str, default=VCF_CONVERSION_SCRIPT, help='The script that converts a vcf file format into quickly comparable format.')
    parser.add_argument('--loci_filter', dest='filter', type=str, default=FILTER_FILE, help='The bed file detailing the loci on which the vcf should be filtered.')
    args = parser.parse_args()
    if args.sample is None:
        directory, filename = os.path.split(args.vcf_file)
        args.sample = filename[0:15]
    if args.output_file is None:
        if args.search_file is None:
            args.output_file = args.sample + '.con'
        else:
            directory, filename = os.path.split(args.search_file)
            basename, extensionname = os.path.splitext(filename)
            args.output_file = args.sample + '_against_' + basename + '.con'
    if args.output_dir is None:
        vcf_directory,vcf_fname = os.path.split(args.vcf_file)
        if vcf_directory == "":
            args.output_dir = os.getcwd()
        else:
            args.output_dir = vcf_directory
    
    sample_key = ensure_sample_format(args.sample)
    pipeline_geno_file = convert_variants_form(args.vcf_file,sample_key,script=args.vcf_conversion_script,filter=args.filter)
    if args.search_file is None:
        affy_geno_file = grab_affy_sample_genotypes_file(sample_key)
        report = report_concordance(affy_geno_file,pipeline_geno_file)
        with open(os.path.join(args.output_dir,args.output_file),'w') as f:
            f.write("same\ttotal\tpercentage\tformat\terrors\n")
            f.write(report)
        print "output file = " + os.path.join(args.output_dir,args.output_file)
        sys.exit()
    with open(args.search_file,'r') as f:
        search_list = f.read().split('\n')
    with open(os.path.join(args.output_dir,args.output_file),'w',0) as f:
        for fname in search_list:
            if fname == "":
                continue
            if not os.path.isfile(fname):
                sys.exit("The following file does not exist: {0}\n".format(fname))
            directory, filename = os.path.split(fname)
            report = report_concordance(fname,pipeline_geno_file)
            f.write(filename[0:15] + "\t" + report + "\n")
    print "output file = " + os.path.join(args.output_dir,args.output_file)
            
#            columns = report[1].split('\t')
#                concordance = "%.4f" % float(columns[3])
#                try:
#                    concord_dict[concordance].append(report[1])
#                except KeyError:
#                    concord_dict[concordance] = []
#                    concord_dict[concordance].append(report[1])
#        cs =[float(c) for c in concord_dict.keys()]
#        cs.sort(reverse=True)
#        concord_keys = ["%.4f" % c for c in cs]
#        for c in concord_keys:
#            for line in concord_dict[str(c)]:
#                f.write(line + "\n")

