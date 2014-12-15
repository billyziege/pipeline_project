import re
import os
import yaml
import argparse
import shutil
from processes.hiseq.sample_sheet import SampleSheetObj

def list_monitoring_dirs(directory):
    """
    Returns a list of directories below the given directory that have three "_"'s in their basename.
    """
    dirs = []
    for p in os.listdir(directory):
        if os.path.isdir(os.path.join(directory,p)):
            if re.search("_\w+_\w+_[A,B]\w\w\w\w\w\w\w\w\w",p):
                dirs.append(os.path.join(directory,p))
    return dirs 

def list_sample_dirs(directories):
    """
    Returns a dictionary of directories below the given list of directories keyed by sample id
    by searching for the first SampleSheet.csv file.
    """
    sample_dirs = {}
    for directory in directories:
        for root, dirs, files in os.walk(directory):
            if not re.search('Sample_',root):
                continue
            for f in files:
                if f == 'SampleSheet.csv':
                    sample_sheet_file = os.path.join(root,f)
                    sample_sheet_obj = SampleSheetObj(sample_sheet_file=sample_sheet_file)
                    sample_sheet_table = sample_sheet_obj.sample_sheet_table
                    samples_list = sample_sheet_table.__get_column_values__("SampleID")
                    if len(samples_list) > 1:
                       continue
                    if re.search('Undetermined',root):
                       continue
                    if not samples_list[0] in sample_dirs:
                        sample_dirs[samples_list[0]] = []
                    sample_dirs[samples_list[0]].append(root)
    return sample_dirs

def list_project_sample_dirs(directories):
    """
    Returns a dictionary of directories below the given list of directories keyed by project then sample id.
    This is the general framework of output from casava.
    """
    project_dirs_obj = {}
    for directory in directories:
        for root, dirs, files in os.walk(directory):
            for d in dirs:
                if not d.startswith("Project_"):
                    continue
                dirname = os.path.basename(d)
                if not dirname in project_dirs_obj:
                    project_dirs_obj[dirname] = {}
                project_dir = os.path.join(root,d)
                sample_dirs_obj = list_sample_dirs([project_dir])
                for sample in sample_dirs_obj:
                    if not sample in project_dirs_obj[dirname]:
                        project_dirs_obj[dirname]["Sample_"+sample] = []
                    for sample_dir in sample_dirs_obj[sample]:
                        project_dirs_obj[dirname]["Sample_"+sample].append(sample_dir)
    return project_dirs_obj

def copy_all_xml(input_dir,output_dir):
    """
    Copies the xml files from the input to the output dir.
    """
    files = os.listdir(input_dir)
    for file in files:
        if os.path.isfile(os.path.join(input_dir,file)):
            if file.endswith('.xml'):
                shutil.copy(os.path.join(input_dir,file),os.path.join(output_dir,file))

def check_fastq_output(flowcell_dir):
    """
    Checks to make sure that the standard output expected after casava is present.
    """
    output = {}
    output["index"] = check_index_counts(flowcell_dir)
    output["fastqc"] = []
    output["md5"] = []
    sample_dirs = list_sample_dirs([flowcell_dir])
    for sample in sample_dirs:
        for directory in sample_dirs[sample]:
            if not check_md5sum(directory):
                output["md5"].append(directory)
            if not check_fastqc_output(directory):
                output["fastqc"].append(directory)
    return output
            

def check_index_counts(flowcell_dir):
    """
    Checks to make sure the flowcell count files have been created. 
    """
    if os.path.isdir(os.path.join(flowcell_dir,"Undetermined_indices")):
        for filename in os.listdir(os.path.join(flowcell_dir,"Undetermined_indices")):
            if filename.endswith("index_counts.txt"):
                return True
    return False

def check_md5sum(sample_dir):
    """
    Checks to make sure the sample dir has the checksum.txt file. 
    """
    for filename in os.listdir(sample_dir):
        if filename.endswith("checksum.txt"):
            return True
    return False

def check_fastqc_output(sample_dir):
    """
    Checks to make sure the sample dir has the fastqc directory. 
    """
    for dirname in os.listdir(sample_dir):
        if dirname == 'fastqc':
            return True
    return False

if __name__ == '__main__':
    #Handle arguments
    parser = argparse.ArgumentParser(description='Test various functions in this package')
    parser.add_argument('path', type=str, help='Provides the path to the flowcell/project/sample directory (Required).')
    parser.add_argument('--monitoring_dirs_list', dest="monitor", action="store_true", default=False, help='Tests the list monitoring dirs function.')
    parser.add_argument('--sample_dirs_list', dest="sample", action="store_true", default=False, help='Tests the list sample dirs function.')
    parser.add_argument('--project_sample_dirs_list', dest="project", action="store_true", default=False, help='Tests the list project sample dirs function.')
    parser.add_argument('--fastq_output', dest="fastq_output", action="store_true", default=False, help='Tests the check fastq output function.')

    args = parser.parse_args()
    if args.monitor:
        print str(list_monitoring_dirs(args.path))
    if args.sample:
        print str(list_sample_dirs([args.path]))
    if args.project:
        print str(list_project_sample_dirs([args.path]))
    if args.fastq_output:
        print str(check_fastq_output(args.path))
