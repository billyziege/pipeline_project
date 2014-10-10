import re
import os
import yaml
import argparse
import shutil
from processes.hiseq.sample_sheet import SampleSheetObjList
from processes.hiseq.scripts import list_sample_dirs
from processes.parsing import parse_sequencing_run_dir

def merge_flowcell_casava_results(flowcell_dirs,output_dir,*args,**kwargs):
    """
    Merges the samples in multiple flowcell directories.
    """
    sample_dirs_dict = list_sample_dirs(flowcell_dirs)
    sample_sheet_obj_list = SampleSheetObjList()
    sample_sheet_obj_list.__load_sample_sheets_from_sample_directories__(sample_dirs_dict)
    merge_casava_fastq_directories(sample_sheet_obj_list,output_dir,meta_data_prefix=["FCID"])
    return

def merge_split_casava_results(split_dir,output_dir,split_categories=["Index_length","Lane"]):
    """
    After split directories are run through casava, this function recombines them.  First, the input dirs are
    inferred, and then data is moved.
    """
    sample_sheet_obj_list = SampleSheetObjList()
    sample_sheet_obj_list.__load_sample_sheets_from_meta_directories__(split_dir,split_categories)
    #Get all sample ids.
    sample_sheet_obj_list = sample_sheet_obj_list.__partition_sample_sheet_objects__("SampleID") #Partition by sample (as casava does)
    sample_sheet_obj_list = sample_sheet_obj_list.__partition_sample_sheet_objects__("SampleProject") #Partition by project (as casava does)
    #Label th input directory for each sample
    for sample_sheet_obj in sample_sheet_obj_list.list:
        split_subdir_pieces = []
        for category in split_categories:
            piece = sample_sheet_obj.__get_meta_datum__(category)
            split_subdir_pieces.append(str(piece))
        split_subdir = "_".join(split_subdir_pieces)
        project_subdir = "Project_" + sample_sheet_obj.__get_meta_datum__("SampleProject")
        sample_subdir = "Sample_" + sample_sheet_obj.__get_meta_datum__("SampleID")
        original_dir = os.path.join(split_dir,split_subdir+"/"+project_subdir+"/"+sample_subdir)
        sample_sheet_obj.__set_meta_datum__("original_dir",original_dir)
    #Do the merging
    merge_casava_fastq_directories(sample_sheet_obj_list,output_dir,merge_type="move")
    move_undetermined_directories_of_min_length(output_dir,sample_sheet_obj_list,"Lane" in split_categories)
    return
        
def move_undetermined_directories_of_min_length(output_dir,sample_sheet_obj_list,merge_by_lane=True):
    """
    Copies the undetermined directories corresponding to the min index length to the output directory.
    """
    if merge_by_length is True:
        min_index_lengths = {} #Lane -> index_length
        for sample_sheet_obj in sample_sheet_obj_list.list:
            lane = sample_sheet_obj.meta_data["Lane"]
            index_length = int(sample_sheet_obj.meta_data["Index_length"])
            split_dir = os.path.join(output_dir,"split/"+str(index_length)+"_"+str(lane))
            if not lane in min_index_lengths:
                min_index_lengths[lane] = index_length
            else:
                if min_index_length[lane] > index_length:
                    min_index_length[lane] = index_length
        undetermined_output_dir = os.path.join(output_dir,'Undetermined_indices')
        for lane in min_index_length:
            undetermined_dir = os.path.join(prev_step.output_dir,"split/"+str(min_index_length[lane])+"_"+str(lane)+"/Undetermined_indices/Sample_lane"+str(lane))
            shutil.copytree(undetermined_dir,undetermined_output_dir)
    else:
        initial = True
        for sample_sheet_obj in sample_sheet_obj_list.list:
            index_length = int(sample_sheet_obj.meta_data["Index_length"])
            split_dir = os.path.join(output_dir,"split/"+str(index_length))
            if initial:
                initial = False
                min_index_length = index_length
            if min_index_length > index_length:
                min_index_length = index_length
        undetermined_dir = os.path.join(prev_step.output_dir,"split/"+str(min_index_length)+"/Undetermined_indices")
        shutil.move(undetermined_dir,output_dir)
    return
            
def merge_casava_fastq_directories(sample_sheet_obj_list,output_dir,merge_type="symbolic_link",meta_data_prefix=[]):
    """
    Takes the sample sheet object with "original_dir" meta data and uses this input dir to correctly output
    directories in casava format within the output dir.
    """
    sample_sheet_obj_list = sample_sheet_obj_list.__partition_sample_sheet_objects__("SampleID") #Partition by sample (as casava does)
    sample_ids = sample_sheet_obj_list.__get_column_values__("SampleID")
    sample_sheet_obj_list = sample_sheet_obj_list.__partition_sample_sheet_objects__("SampleProject") #Partition by project (as casava does)
    for meta_key in meta_data_prefix:
        sample_sheet_obj_list = sample_sheet_obj_list.__partition_sample_sheet_objects__(meta_key) #Partition by key used for labeling
    for sample_id in sample_ids:
        specific_sample_sheet_obj_list = sample_sheet_obj_list.__filter_sample_sheet_objects__({"SampleID": sample_id}) #Do each sample separately.
        project_ids = specific_sample_sheet_obj_list.__get_column_values__("SampleProject")
        sample_output_dir = os.path.join(output_dir,"Project_"+project_ids[0]+"/Sample_"+sample_id)
        single_sample_sheet_obj_list = specific_sample_sheet_obj_list.__merge_all_sample_sheet_objects__()
        if not os.path.isdir(sample_output_dir):
            os.makedirs(sample_output_dir)
        single_sample_sheet_obj_list.list[0].sample_sheet_table.__write_file__(os.path.join(sample_output_dir,"SampleSheet.csv"))
        for sample_sheet_obj in sample_sheet_obj_list.list:
            input_dir = sample_sheet_obj.__get_meta_datum__("original_dir")
            for filename in os.listdir(input_dir):
                if filename.endswith('fastq.gz'):
                    output_filename_pieces= []
                    for meta_key in meta_data_prefix:
                        piece = sample_sheet_obj.__get_meta_datum__(meta_key)
                        output_filename_pieces.append(str(piece))
                    output_filename_pieces.append(filename)
                    output_filename = "_".join(output_filename_pieces)
                    input_path = os.path.join(input_dir,filename)
                    output_path = os.path.join(sample_output_dir,output_filename)
                    if merge_type == "symbolic_link":
                        os.symlink(input_path,output_path)
                    if merge_type == "move":
                        shutil.move(input_path,output_path)
                    if merge_type ==  "copy":
                        shutil.copy(input_path,output_path)
    return


if __name__ == '__main__':
    #Handle arguments
    parser = argparse.ArgumentParser(description='Test various functions in this package')
    parser.add_argument('output_dir', type=str, help='The output dir where the casava output will be built. (Required).')
    parser.add_argument('--merge_flowcells', dest='flowcell_dirs', nargs='+', help='Multiple paths to flowcells to be merged.')
    parser.add_argument('--merge_type', dest='merge_type', type=str, help='The type of operation used during merging.  Default is "symbolic_link".  Options are "symbolic_link", "move", and "copy".',default="symbolic_link")
    args = parser.parse_args()
 
    if args.flowcell_dirs:
        merge_flowcell_casava_results(args.flowcell_dirs,args.output_dir,args.merge_type)
