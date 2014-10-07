import re
import os
import yaml
import argparse
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
            for f in files:
                if f == 'SampleSheet.csv':
                    sample_sheet_file = os.path.join(root,f)
                    sample_sheet_obj = SampleSheetObj(sample_sheet_file=sample_sheet_file)
                    sample_sheet_table = sample_sheet_obj.sample_sheet_table
                    samples_list = sample_sheet_table.__get_column_values__("SampleID")
                    if len(samples_list) > 1:
                       continue
                    if not samples_list[0] in sample_dirs:
                        sample_dirs[samples_list[0]] = []
                    sample_dirs[samples_list[0]].append(root)
    return sample_dirs

def list_project_sample_dirs(directories):
    """
    Returns a dictionary of directories below the given list of directories keyed by project then sample id.
    One of the directories at the first level below the given directories must start with Project_.  This
    is the general framework of output from casava.
    """
    project_dirs_obj = {}
    for dir in directories:
        for dirname in os.listdir(dir):
            if not os.path.isdir(os.path.join(dir,dirname)):
                continue
            if not dirname.startswith("Project_"):
                continue
            if not dirname in project_dirs_obj:
                project_dirs_obj[dirname] = {}
            project_dir = os.path.join(dir,dirname)
            sample_dirs_obj = list_sample_dirs([project_dir])
            for sample in sample_dirs_obj:
                if not sample in project_dirs_obj[dirname]:
                    project_dirs_obj[dirname][sample] = []
                for sample_dir in sample_dirs_obj[sample]:
                    project_dirs_obj[dirname][sample].append(sample_dir)
    return project_dirs_obj

if __name__ == '__main__':
    #Handle arguments
    parser = argparse.ArgumentParser(description='Test various functions in this package')
    parser.add_argument('path', type=str, help='Provides the path to the flowcell/project/sample directory (Required).')
    parser.add_argument('--monitoring_dirs_list', dest="monitor", action="store_true", default=False, help='Tests the list monitoring dirs function.')
    parser.add_argument('--sample_dirs_list', dest="sample", action="store_true", default=False, help='Tests the list sample dirs function.')
    parser.add_argument('--project_sample_dirs_list', dest="project", action="store_true", default=False, help='Tests the list project sample dirs function.')

    args = parser.parse_args()
    if args.monitor:
        print str(list_monitoring_dirs(args.path))
    if args.sample:
        print str(list_sample_dirs([args.path]))
    if args.project:
        print str(list_project_sample_dirs([args.path]))
