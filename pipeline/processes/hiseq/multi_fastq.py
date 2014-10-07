import re
import os
import yaml
import argparse

def create_multi_fastq_yaml(yaml_filename,directory_list):
    """
    Wraps the object so that it is stored as a yaml file (and
    therefore can be later retrieved.
    """
    with open(yaml_filename, 'w') as outfile:
        outfile.write( yaml.dump(create_multi_fastq_object(directory_list),default_flow_style=False) )

def list_from_multi_fastq_object(multi_fastq,attribute):
    """
    Most of the attributes for the multi fastq object will
    be manipulated on as an array.  This converts the given
    attribute to an array.
    """
    attribute_list = []
    for i in range(len(multi_fastq)):
        attribute_list.append(multi_fastq[i][attribute])
    return attribute_list

def create_multi_fastq_object(directory_list):
    """
    Goes through a list of directories, pairs pair end read
    files, and attaches lane/flowcell information.  It is assumed
    that only a single sample is in the sample sheet.
    """
    multi_fastq = []
    for directory in directory_list:
        sample_sheet_table =  csv.DictReader(open(os.path.join(directory, 'SampleSheet.csv'),delimiter=',')
        flowcell_key = sample_sheet_table[0]["FCID"]
        sample_key = sample_sheet_table[0]["SampleID"]
        for p in os.listdir(directory):
            if re.search(r"_R1_",p):
                if p.endswith(".fastq") or p.endswith(".fastq.gz"):
                    fastq_entry = {"r1_filename": os.path.join(directory,p)}
                    (fastq_entry["lane"], read_group) = parse_fastq_filename(p)
                    r2_filename = re.sub(r"_R1_","_R2_",p)
                    if os.path.isfile(os.path.join(directory,r2_filename)):
                        fastq_entry["r2_filename"] = os.path.join(directory,r2_filename)
                    fastq_entry["flowcell"] = flowcell_key
                    fastq_entry["sample"] = sample_key
                    multi_fastq.append(fastq_entry)
    return multi_fastq
        

def parse_fastq_filename(p):
    """
    Returns the read group and lane of the fastq file in the standard format provided
    by Illumina
    """
    lane_match = re.search(r"L00([1-8])",p)
    if lane_match:
        lane = lane_match.group(1)
    else:
        lane = 1
    read_group_match = re.search(r"R([1,2])",p)
    if read_group_match:
        read_group = read_group_match.group(1)
    else:
        read_group = 1
    return (lane, read_group)
    
if __name__ == '__main__':
    #Handle arguments
    parser = argparse.ArgumentParser(description='Test the parsing of the fastq filename by listing all the information for all fastq files in a single directory')
    parser.add_argument('directory', type=str, help='The path to where the fastq live')
    args = parser.parse_args()
    print str(create_multi_fastq_yaml([args.directory]))
