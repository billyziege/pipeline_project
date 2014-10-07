import re
import os
import yaml
import csv
import argparse
from sge_email.scripts import send_email

class SampleSheetFormatException(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)
    def __unicode__(self):
        return repr(self.value)

class MyTable():
    """
    Provides an accessible framework to access a table by adding the method fieldnames (similar to 
    csv reader.
    """

    def __init__(self):
        """
        Rows with fieldnames, similar to a dict reader obj from csv.
        """
        self.rows = []
        self.fieldnames = []
  
    def __add_file__(self,path,append=True):
        """
        Reads the path as if it were a sample sheet and handles cleaning.
        """
        dict_reader_obj = csv.DictReader(open(path,'r'),delimiter=',')
        if not append:
           self.rows = []
        self.__inject_dict_reader_obj__(dict_reader_obj)

    def __write_file__(self,path,**kwargs):
        """
        Writes the table to the path
        """
        csvwriter = csv.DictWriter(open(path,'w'),fieldnames=self.fieldnames,**kwargs)
        csvwriter.writeheader()
        for row in self.rows:
            csvwriter.writerow(dict(zip(self.fieldnames,row)))

    def __print__(self,delimiter=","):
        """
        Prints the table to stdout
        """
        print delimiter.join(self.fieldnames)
        for row in self.rows:
            print delimiter.join(row)

    def __inject_dict_reader_obj__(self,dict_reader_obj):
        if self.fieldnames == []:
            self.fieldnames = dict_reader_obj.fieldnames
        for row in dict_reader_obj:
            outrow = []
            for field in self.fieldnames:
                outrow.append(row[field])
            self.rows.append(outrow)

    def __copy_fieldnames__(self):
        return self.fieldnames.copy()

    def __add_row__(self,row):
        return self.rows.append(row)

    def __get_field_index__(self,column_name):
        """
        Returns the index of the column_name in fieldnames.
        """
        return self.fieldnames.index(column_name)

    def __get_column_values__(self,column_name,distinct=True):
        """
        Provides a list of all values in a column.  If distinct 
        is True, this list has each element appearing only once.
        """
        values = []
        column_index = self.__get_field_index__(column_name)
        for row in self.rows:
            if distinct and row[column_index] in values:
                continue
            values.append(row[column_index])
        return values

class SampleSheetObj():
    """
    Stores the sample sheet and meta data about the sample sheet in
    a dictionary.  This allows partioning of the sample sheet.
    """

    def __init__(self,sample_sheet_table=None,sample_sheet_file=None,meta_data={}):
        """
        Stores the (optional) original sample sheet in the sample sheet obj
        and adds any meta data that may be attached to it. 
        """
        if sample_sheet_table is None:
            sample_sheet_table = MyTable()
            if not sample_sheet_file is None:
                sample_sheet_table.__add_file__(sample_sheet_file)
        self.__set_sample_sheet_table__(sample_sheet_table)
        self.__clean_sample_sheet__()
        self.meta_data = meta_data.copy()

    def __set_sample_sheet_table__ (self,sample_sheet_table, overwrite = False):
        """
        Adds the sample sheet table to the sample sheet object.
        """
        if overwrite or not hasattr(self,"sample_sheet_table") or len(self.sample_sheet_table.rows) == 0:
            self.sample_sheet_table = sample_sheet_table
            return
        raise Exception("Failed trying to overwrite a sample sheet that already had data.")

    def __clean_sample_sheet__(self):
        """
        Standard treatment for cleaning a sample sheet.
        """
        sampleid_index = self.sample_sheet_table.__get_field_index__("SampleID")
        index_index = self.sample_sheet_table.__get_field_index__("Index")
        for row in self.sample_sheet_table.rows:
            row[sampleid_index] = clean_sample_name(row[sampleid_index])
            row[index_index] = clean_index(row[index_index])
             
    def __get_sample_sheet_table__(self):
        """
        Retrieves the sample sheet table.
        """
        return self.sample_sheet_table

    def __set_meta_datum__(self,key,value,overwrite = False):
        """
        Adds attributes to the sample sheet obj.
        """
        if not key in self.meta_data or overwrite:
            self.meta_data[key]=value
            return
        raise Exception("Failed trying to overwrite meta data for a sample sheet that already had such meta data.")

    def __get_meta_datum__(self,key):
        """
        Retrieves the meta datum of the sample sheet obj.
        """
        if key in self.meta_data:
            return self.meta_data[key]
        raise Exception("The key "+key+" is not in the meta data of the sample sheet object")

    def __string_from_meta_data__(self):
        """
        Prints a string from the meta data
        """
        output = []
        for key in sorted(self.meta_data.keys()):
            output.append(str(self.meta_data[key]))
        return "_".join(output)

    def __print__(self,delimiter=","):
        """
        Prints a line for the meta data and then the table content.
        """
        print self.__string_from_meta_data__()
        self.sample_sheet_table.__print__()
       

    def __filter_sample_sheet_table__(self,column_name,value,use_length=False):
        """
        Returns a new sample sheet table only containing rows where the column with column 
        name has the provided value.  If use length is True, then the value is the length
        of the value, not the value itself.
        """
        new_sample_sheet_table = MyTable()
        new_sample_sheet_table.fieldnames = self.sample_sheet_table.fieldnames
        column_index = self.sample_sheet_table.__get_field_index__(column_name)
        for row in self.sample_sheet_table.rows:
            if not use_length and row[column_index] == value:
                new_sample_sheet_table.__add_row__(row)
            if use_length and len(row[column_index]) == value:
                new_sample_sheet_table.__add_row__(row)
        return new_sample_sheet_table


class SampleSheetObjList():
    """
    Provides a container that holds sample sheet objects and provides functional access to them.
    """

    def __init__(self,sample_sheet_table=None,sample_sheet_file=None,meta_data={}):
        """
        Just a list with an possible initial element.
        """
        self.list = []
        if not sample_sheet_table is None or not sample_sheet_file is None:
            self.__add_new_sample_sheet_object__(sample_sheet_table,sample_sheet_file,meta_data)

    def __add_new_sample_sheet_object__(self,sample_sheet_table=None,sample_sheet_file=None,meta_data={}):
        new_sample_sheet_obj = SampleSheetObj(sample_sheet_table,sample_sheet_file,meta_data)
        self.list.append(new_sample_sheet_obj)
        return new_sample_sheet_obj

    def __load_sample_sheets_from_meta_directories__(self,base_dir,meta_keys):
        """
        Loads the sample sheets according to their directory structure and
        the provided meta keys.
        This does the opposite of the create meta directories and write files below.
        """
        for directory in os.listdir(base_dir):
            path = os.path.join(base_dir,directory)
            if not os.path.isdir(path):
                continue
            if not os.path.isfile(os.path.join(path,"SampleSheet.csv")):
                continue
            meta_values = directory.split("_")
            if len(meta_values) != len(meta_keys):
                raise SampleSheetFormattingError("The provided meta keys " + str(meta_keys) + " and the derived meta values " + str(meta_values) + "are not in agreement.")
            meta_data = dict(zip(meta_keys,meta_values))
            self.__add_new_sample_sheet_object__(sample_sheet_file=os.path.join(path,"SampleSheet.csv"),meta_data=meta_data)
            

    def __filter_sample_sheet_objects__(self,meta_data):
        """
        Returns a new sample sheet object list holding all of the sample sheet objects
        that meet the meta data dictionary restrictions.  I.e.  this filters on the meta data.
        """
        new_sample_sheet_obj_list = SampleSheetObjList()
        for sample_sheet_obj in self.list:
            for key in meta_data:
                try:
                    value = sample_sheet_obj.__get_meta_datum__(key)
                    if value == meta_data[key]:
                        new_sample_sheet_obj_list.list.append(sample_sheet_obj) #This does not copy.  It is the same object!
                except:
                    continue
        return new_sample_sheet_obj_list

    def __partition_sample_sheet_objects__(self,column_name,use_length=False):
        """
        Returns a new sample sheet object list with the sample sheet table partitioned into separate objects by the 
        column name provided.  The column name then is stored as meta data in each of the sample sheet object.
        If use length is set to true, then the set of lengths of the column's value is used, not the
        value itself.
        """
        new_sample_sheet_obj_list = SampleSheetObjList()
        for sample_sheet_obj in self.list:
            values = sample_sheet_obj.sample_sheet_table.__get_column_values__(column_name) #Determines the partition
            if use_length:
                values = list(set([len(v) for v in values])) #Unique list for partition
            for value in values:
                new_sample_sheet_obj = new_sample_sheet_obj_list.__add_new_sample_sheet_object__(sample_sheet_table=sample_sheet_obj.__filter_sample_sheet_table__(column_name,value,use_length),meta_data=sample_sheet_obj.meta_data)
                new_meta_label = column_name
                if use_length:
                    new_meta_label += "_length"
                new_sample_sheet_obj.__set_meta_datum__(new_meta_label,value)
        return new_sample_sheet_obj_list

    def __create_meta_directories_and_write_files__(self,base_dir,**kwargs):
        """
        Writes the sample sheets in the list object to sub directories defined by the
        metadata.
        """
        output_dirs = []
        for sample_sheet_obj in self.list:
            output_dir = os.path.join(base_dir,sample_sheet_obj.__string_from_meta_data__())
            if not os.path.isdir(output_dir):
                os.makedirs(output_dir)
            sample_sheet_obj.sample_sheet_table.__write_file__(os.path.join(output_dir,"SampleSheet.csv"))
            output_dirs.append(output_dir)
        return output_dirs

    def __print__(self):
        """
        Prints all sample sheets and meta data in the list.
        """
        for sample_sheet_obj in self.list:
            sample_sheet_obj.__print__()

def clean_sample_name(orig_sample_name):
    """
    Clients are sending us samples that are breaking casava.  We remove
    all trailing spaces, and replace all spaces and special characters with
    a "-".
    """
    sample_name = orig_sample_name.strip()
    special_characters = [' ','!','@','#','$', '%', '^', '&', '*', '(', ')', '{', '}', '[', ']', '+', '=', '\\', '/', ':', ';', '"', "'", ',', '?', '<', '>']
    for special_character in special_characters:
        sample_name, number = re.subn(re.escape(special_character),'-',sample_name)
    return sample_name

def clean_index(index):
    """
    Indexes often come with n's at the end when samples with different length indices (indexes?) are
    pooled together.
    """
    return index.rstrip('n')

def translate_sample_name(orig_sample_name):
    """
    The bcbio pipeline does not work with samples that begin with a number,
    so this was an initial attempt to rename and clean the sample name
    specifically for the MSBP project.
    """
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

def send_missing_sample_sheet_email(sample_sheet_file):
    message =str(sample_sheet)+" is missing.  Casava cannot run.\n"
    send_email("Missing "+str(sample_sheet),message,recipients='zerbeb@humgen.ucsf.edu,Dedeepya.Vaka@ucsf.edu,LaoR@humgen.ucsf.edu')

if __name__ == '__main__':
    #Handle arguments
    parser = argparse.ArgumentParser(description='Test various functions in this package')
    parser.add_argument('--sample_sheet', dest="sample_sheet", type=str, help='Provides the path for the sample sheet.')
    parser.add_argument('--load', dest="load", action="store_true", default=False, help='Tests the load function.')
    parser.add_argument('--partition', dest="partition", action="store_true", default=False, help='Tests the partition function.')
    parser.add_argument('--write', dest="write", default=None, help='Writes the sample sheet(s) to the specified directory using meta data.')
    parser.add_argument('--load_multiple', dest="load_multiple", default=None, help='Reads the meta data sub-directories of the specified directory and prints them.')

    args = parser.parse_args()
    sample_sheet_obj_list = SampleSheetObjList()
    if args.load:
        sample_sheet_obj_list.__add_new_sample_sheet_object__(sample_sheet_file=args.sample_sheet)
        sample_sheet_obj_list.__print__()
    if args.partition:
        sample_sheet_obj_list.__add_new_sample_sheet_object__(sample_sheet_file=args.sample_sheet)
        new_list = sample_sheet_obj_list.__partition_sample_sheet_objects__("Index",use_length=True).__partition_sample_sheet_objects__("Lane")
        if args.write is None:
            new_list.__print__()
        else:
            new_list.__create_directories_and_write_files__(args.write)
    if not args.load_multiple is None:
        sample_sheet_obj_list.__load_sample_sheets_from_meta_directories__(args.load_multiple,["Index","Lane"])
        sample_sheet_obj_list.__print__()
