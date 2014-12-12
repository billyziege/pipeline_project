import re
import os
import yaml
import csv
import argparse
from sge_email.scripts import send_email
from processes.hiseq.seq_read import SeqReadSet

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
    csv reader.)
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

    def __merge_tables__(self,other):
        """
        If the two tables hav the same fieldnames, a new table is returned
        containing the rows of the first then second tables.
        """
        if len(self.fieldnames) != 0 and len(other.fieldnames) != 0:
            if set(self.fieldnames) != set(other.fieldnames):
                raise SampleSheetFormatException("Trying to merge two tables with different fieldnames.")
        new_table = MyTable()
        if len(self.fieldnames) != 0:
            new_table.fieldnames = self.__copy_fieldnames__()
        else:
            new_table.fieldnames = other.__copy_fieldnames__()
        for row in self.rows:
            new_table.__add_row__(row)
        for row in other.rows:
            new_table.__add_row__(row)
        return new_table
        
    def __write_file__(self,path,**kwargs):
        """
        Writes the table to the path
        """
        csvwriter = csv.DictWriter(open(path,'w'),fieldnames=self.fieldnames,**kwargs)
        csvwriter.writeheader()
        for row in self.rows:
            csvwriter.writerow(dict(zip(self.fieldnames,row)))

    def __print__(self,delimiter=",",*args,**kwargs):
        """
        Prints the table to stdout
        """
        print delimiter.join(self.fieldnames)
        for row in self.rows:
            print delimiter.join(row)

    def __inject_dict_reader_obj__(self,dict_reader_obj):
        """
        Converts the csv.DictReader object to MyTable.
        """
        if self.fieldnames == []:
            self.fieldnames = dict_reader_obj.fieldnames
        for row in dict_reader_obj:
            outrow = []
            for field in self.fieldnames:
                outrow.append(row[field])
            self.rows.append(outrow)

    def __copy_fieldnames__(self):
        fieldnames = []
        for fieldname in self.fieldnames:
            fieldnames.append(fieldname)
        return fieldnames

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
        if "SampleID" in self.sample_sheet_table.fieldnames and "Index" in self.sample_sheet_table.fieldnames:
            sampleid_index = self.sample_sheet_table.__get_field_index__("SampleID")
            index_index = self.sample_sheet_table.__get_field_index__("Index")
            for row in self.sample_sheet_table.rows:
                row[sampleid_index] = clean_sample_name(row[sampleid_index])
                index_pieces = []
                for index_piece in row[index_index].split('-'):
                    cleaned_index = clean_index(index_piece)
                    if len(cleaned_index) > 0:
                        index_pieces.append(cleaned_index)
                row[index_index] = "-".join(index_pieces)
             
    def __get_sample_sheet_table__(self):
        """
        Retrieves the sample sheet table.
        """
        return self.sample_sheet_table

    def __merge_sample_sheet_objects__(self,other,new_obj_meta_data={}):
        """
        Returns a new object only with new meta data and containg the merged tables from the two provided objects.
        """
        new_sample_sheet_obj = SampleSheetObj()
        new_sample_sheet_obj.sample_sheet_table = self.sample_sheet_table.__merge_tables__(other.sample_sheet_table)
        new_sample_sheet_obj.meta_data = new_obj_meta_data.copy()
        return new_sample_sheet_obj
        
    def __attach_mask__(self,seq_read_set=None,run_parameters_path=None):
        """
        Place the mask info into the meta data dict.
        """
        if seq_read_set is None:
            if run_parameters_path is None:
                return
            seq_read_set = SeqReadSet(run_parameters_path)
        if self.__has_meta_datum__("Index_length"):
            number_reads = len(seq_read_set.seq_reads)
            index_lengths = self.__get_meta_datum__("Index_length").split('-')
            for i in range(1,number_reads-1):
                seq_read = seq_read_set.seq_reads[i]
                try:
                    seq_read.__set_actual_length__(index_lengths[i-1])
                except IndexError: ##None '-' index in same flowcell as '-' index 
                    seq_read.__set_actual_length__(0)
        mask = seq_read_set.__write_as_string__() 
        self.__set_meta_datum__("mask",mask)
 
    def __set_meta_datum__(self,key,value,overwrite = True):
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

    def __has_meta_datum__(self,key):
        """
        Return true if the key exists in the meta_data dict.
        """
        return key in self.meta_data
    def __string_from_meta_data__(self,meta_keys=None):
        """
        Prints a string from the meta data
        """
        output = []
        if meta_keys is None:
            meta_keys = self.meta_data.keys()
        for key in sorted(meta_keys):
            output.append(str(self.meta_data[key]))
        return "_".join(output)

    def __print__(self,print_meta_data=True,*args,**kwargs):
        """
        Prints a line for the meta data and then the table content.
        """
        if print_meta_data is True:
            print self.__string_from_meta_data__(*args,**kwargs)
        self.sample_sheet_table.__print__(*args,**kwargs)
       

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
            if use_length:
                pieces = row[column_index].split("-")
                pieces_length = [str(len(piece)) for piece in pieces]
                for i in range(len(pieces_length),len(value.split('-'))):
                    pieces_length.append("0")
                pieces_string = "-".join(pieces_length)
                if pieces_string == str(value):
                    new_sample_sheet_table.__add_row__(row)
        return new_sample_sheet_table

    def __attach_max_column_number__(self,column_name,delimiter='-'):
        """
        Puts the max number of strings separated by the delimiter into the meta data.
        """
        column_index = self.sample_sheet_table.__get_field_index__(column_name)
        max_number = 0
        for row in self.sample_sheet_table.rows:
            values = row[column_index].split(delimiter)
            column_number = len(values)
            if column_number > max_number:
                max_number = column_number
        self.__set_meta_datum__(column_name+"_number",max_number)
        return max_number
        

class SampleSheetObjList():
    """
    Provides a container that holds sample sheet objects and provides functional access to them.
    """

    def __init__(self,sample_sheet_table=None,sample_sheet_file=None,meta_data={}):
        """
        Just a list with an possible initial element.
        """
        self.list = []
        self.__add_new_sample_sheet_object__(sample_sheet_table,sample_sheet_file,meta_data)

    def __add_sample_sheet_obj__(self,sample_sheet_obj):
        """
        Adds the provided sample sheet object to the list.
        """
        if len(self.list) == 1:
            if len(self.list[0].sample_sheet_table.fieldnames) == 0:
                self.list = []
        self.list.append(sample_sheet_obj)
        return

    def __add_new_sample_sheet_object__(self,sample_sheet_table=None,sample_sheet_file=None,meta_data={}):
        """
        Adds a new sample sheet object to the list.  If the first object has no fielnames (no real object), this object is removed.
        """
        new_sample_sheet_obj = SampleSheetObj(sample_sheet_table,sample_sheet_file,meta_data)
        self.__add_sample_sheet_obj__(new_sample_sheet_obj)
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
            meta_data.update({"original_dir": path})
            self.__add_new_sample_sheet_object__(sample_sheet_file=os.path.join(path,"SampleSheet.csv"),meta_data=meta_data)
            
    def __load_sample_sheets_from_sample_directories__(self,sample_dirs_dict):
        """
        Load the sample sheets for the give samples in the sample_dirs_dict
        storing the sampleid as meta_data.
        """
        for sample in sample_dirs_dict:
            meta_data = { "SampleID" : sample }
            for dir in sample_dirs_dict[sample]:
                meta_data.update({ "original_dir" : dir })
                sample_sheet_file = os.path.join(dir,"SampleSheet.csv")
                self.__add_new_sample_sheet_object__(sample_sheet_file=sample_sheet_file,meta_data=meta_data)

    def __filter_sample_sheet_objects__(self,filter_meta_data):
        """
        Returns a new sample sheet object list holding all of the sample sheet objects
        that meet the meta data dictionary restrictions.  I.e.  this filters on the meta data.
        """
        new_sample_sheet_obj_list = SampleSheetObjList()
        for sample_sheet_obj in self.list:
            for key in filter_meta_data:
                try:
                    value = sample_sheet_obj.__get_meta_datum__(key)
                    if value == filter_meta_data[key]:
                        new_sample_sheet_obj_list.__add_sample_sheet_obj__(sample_sheet_obj) #This does not copy.  It is the same object!
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
                if not sample_sheet_obj.__has_meta_datum__(column_name+"_number"):
                    raise SampleSheetFormatException("The sample sheet does not have the number to expect for column " + column_name)
                values_temp = []
                for value in values:
                    pieces = value.split("-") #Indexes sometimes have a "-".  Casava interprets these as separate reads.
                    pieces_length = [str(len(piece)) for piece in pieces]
                    for i in range(len(pieces_length),int(sample_sheet_obj.__get_meta_datum__(column_name+"_number"))):
                        pieces_length.append(str(0)) #Multiplexing single indexed samples with mutli-indexed samples requires single indexed samples have 0 in the actual length of additional reads.
                    values_temp.append("-".join(pieces_length))
                values = set(values_temp) #Unique list for partition
            for value in values:
                new_sample_sheet_obj = new_sample_sheet_obj_list.__add_new_sample_sheet_object__(sample_sheet_table=sample_sheet_obj.__filter_sample_sheet_table__(column_name,value,use_length),meta_data=sample_sheet_obj.meta_data)
                new_meta_label = column_name
                if use_length:
                    new_meta_label += "_length"
                new_sample_sheet_obj.__set_meta_datum__(new_meta_label,value)
        return new_sample_sheet_obj_list

    def __create_meta_directories_and_write_files__(self,base_dir,*args,**kwargs):
        """
        Writes the sample sheets in the list object to sub directories defined by the
        metadata.
        """
        output_dirs = []
        for sample_sheet_obj in self.list:
            output_dir = os.path.join(base_dir,sample_sheet_obj.__string_from_meta_data__(*args,**kwargs))
            if not os.path.isdir(output_dir):
                os.makedirs(output_dir)
            sample_sheet_obj.sample_sheet_table.__write_file__(os.path.join(output_dir,"SampleSheet.csv"))
            output_dirs.append(output_dir)
        return output_dirs

    def __merge_all_sample_sheet_objects__(self,new_obj_meta_data={}):
        """
        Returns a new sample sheet object list with all tables merged into a single object.
        """
        new_sample_sheet_object_list = SampleSheetObjList()
        for sample_sheet_object in self.list:
            new_sample_sheet_object_list.list[0] = new_sample_sheet_object_list.list[0].__merge_sample_sheet_objects__(sample_sheet_object)
        new_sample_sheet_object_list.list[0].meta_data = new_obj_meta_data.copy()
        return new_sample_sheet_object_list


    def __print__(self,*args,**kwargs):
        """
        Prints all sample sheets and meta data in the list.
        """
        for sample_sheet_obj in self.list:
            sample_sheet_obj.__print__(*args,**kwargs)

    def __get_column_values__(self,column_name):
        """
        Returns all values of the given column (fieldname) within all of the sample_sheet_tables
        in the list.
        """
        column_values = []
        for sample_sheet_obj in self.list:
            current_column_values = sample_sheet_obj.sample_sheet_table.__get_column_values__(column_name)
            for value in current_column_values:
                if not value in column_values:
                    column_values.append(value)
        return column_values

    def __attach_masks__(self,seq_read_set=None,run_parameters_path=None):
        """
        Place the mask info into the meta data dict.
        """
        if seq_read_set is None:
            if run_parameters_path is None:
                return
            seq_read_set = SeqReadSet(run_parameters_path)
        for sample_sheet_obj in self.list:
            sample_sheet_obj.__attach_mask__(seq_read_set)
        
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
        for sample_sheet_obj in sample_sheet_obj_list.list:
            sample_sheet_obj.__attach_max_column_number__("Index")
        new_list = sample_sheet_obj_list.__partition_sample_sheet_objects__("Index",use_length=True).__partition_sample_sheet_objects__("Lane")
        if args.write is None:
            new_list.__print__(meta_keys=["Index_length","Lane"])
        else:
            new_list.__create_meta_directories_and_write_files__(args.write,meta_keys=["Index_length","Lane"])
    if not args.load_multiple is None:
        sample_sheet_obj_list.__load_sample_sheets_from_meta_directories__(args.load_multiple,["Index","Lane"])
        sample_sheet_obj_list.__print__()
