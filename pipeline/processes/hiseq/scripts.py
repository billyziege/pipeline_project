import re
import os
import yaml
import argparse

#This funciton insures the appropriate name when reading in a sample.
def translate_sample_name(orig_sample_name):
    sample_name = re.sub("_","-",orig_sample_name)
    if sample_name[0:4] != 'K-ND':
        match_object = re.search("(\d+)_([A-H])(\d+$)",sample_name)
        if match_object:
            plate_num = match_object.group(1)
            while len(plate_num) < 5:
                plate_num = '0' + str(plate_num) 
            well_column = match_object.group(2)
            well_row = match_object.group(3)
            if len(well_row) == 1:
                well_row = '0' + str(well_row)
            form = 'K-NDNA' + plate_num + '_' + well_column + well_row
            return form
        else:
            try:
                sample_name.replace(" ","_");
                int(sample_name[0])
                sample_name = "Sample_" + str(sample_name)
            except:
                pass
            return sample_name
    else:
        try:
            sample_name.replace(" ","_");
            int(sample_name[0])
            sample_name = "Sample_" + str(sample_name)
        except:
            pass
        return sample_name

def list_monitoring_dirs(directory):
    dirs = []
    for p in os.listdir(directory):
        if os.path.isdir(os.path.join(directory,p)):
            if re.search("_\w+_\w+_\w+",p):
                dirs.append(os.path.join(directory,p))
    return dirs 

def list_sample_dirs(directories):
    sample_dirs = {}
    for directory in directories:
        for root, dirs, files in os.walk(directory):
            for f in files:
                if f == 'SampleSheet.csv':
                    if re.search('Undetermine', root):
                        continue
                    with open(os.path.join(root,f),'r') as handle:
                        if len(handle.readlines()) > 3:
                            continue 
                    sample_sheet_table  = table_reader(os.path.join(root, f))
                    if not sample_sheet_table[0]["SampleID"] in sample_dirs:
                        sample_dirs[sample_sheet_table[0]["SampleID"]] = []
                    sample_dirs[sample_sheet_table[0]["SampleID"]].append(root)
    return sample_dirs

def table_reader(fname,sep=','):
    """
    Reads a table csv file with a header into a list
    which has a dictionary keyed by row number.
    """
    rows = []
    with open(fname, "r") as f:
        keys = f.readline().strip().split(',')
        for line in f:
            values = line.strip().split(',')
            dictionary = dict(zip(keys, values))
            rows.append(dictionary)
    return rows

