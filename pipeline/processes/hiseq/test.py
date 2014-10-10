from processes.hiseq.sample_sheet import SampleSheetObjList
from processes.hiseq.scripts import list_sample_dirs
import argparse

if __name__ == '__main__':
    #Handle arguments
    parser = argparse.ArgumentParser(description='Test various functions in this functions in this folder that require multiple modules')
    parser.add_argument('--load_samples_sample_sheets', dest="samples_dir", type=str, help='Test the loading of sample sheets by sample by providing the path for under which all sub-directories are evaluated for SampleSheet.csv.')
    parser.add_argument('--column_values', dest="values_dir", type=str, help='Test the column values function by returning a list of samples in all of the sample sheets by providing the path for under which all sub-directories are evaluated for SampleSheet.csv.')
    parser.add_argument('--merge_to_single', dest="merge_dir", type=str, help='Test the merge all sample sheet objects function by returning a single sample sheet by providing the path for under which all sub-directories are evaluated for SampleSheet.csv.')
    parser.add_argument('--filter_by_sample', dest="filter_dir", type=str, help='Test the filter sample sheet object by printing multiple sample sheet objects after providing the path for under which all sub-directories are evaluated for SampleSheet.csv.')

    args = parser.parse_args()
    sample_sheet_obj_list = SampleSheetObjList()
    if args.samples_dir:
        sample_dirs_dict = list_sample_dirs([args.samples_dir])
        sample_sheet_obj_list. __load_sample_sheets_from_sample_directories__(sample_dirs_dict)
        sample_sheet_obj_list.__print__()
    if args.values_dir:
        sample_dirs_dict = list_sample_dirs([args.values_dir])
        sample_sheet_obj_list. __load_sample_sheets_from_sample_directories__(sample_dirs_dict)
        print str(sample_sheet_obj_list.__get_column_values__("SampleID"))
    if args.merge_dir:
        sample_dirs_dict = list_sample_dirs([args.merge_dir])
        sample_sheet_obj_list. __load_sample_sheets_from_sample_directories__(sample_dirs_dict)
        new_sample_sheet_obj_list = sample_sheet_obj_list.__merge_all_sample_sheet_objects__()
        new_sample_sheet_obj_list.__print__()
    if args.filter_dir:
        sample_dirs_dict = list_sample_dirs([args.filter_dir])
        sample_sheet_obj_list. __load_sample_sheets_from_sample_directories__(sample_dirs_dict)
        sample_ids = sample_sheet_obj_list.__get_column_values__("SampleID")
        sample_sheet_obj_list = sample_sheet_obj_list.__partition_sample_sheet_objects__("SampleProject")
        #For each sample id, gather the sample sheets.
        for sample_id in sample_ids:
            specific_sample_sheet_obj_list = sample_sheet_obj_list.__filter_sample_sheet_objects__({"SampleID": sample_id})
            specific_sample_sheet_obj_list.__print__()
