import re
import os
try:
    import xml.etree.cElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET
import argparse

class SeqRead():
    """
    Stores information about a specific read.
    """

    def __init__(self,read_type,num_cycles,actual_length=None):
        """
        Just the basic read attributes of type, length, and actual legnth.
        """
        self.type = read_type
        self.length = int(num_cycles)
        if actual_length is None:
            self.__set_actual_length__(self.length)
        else:
            self.__set_actual_length__(actual_length)

    def __set_actual_length__(self,actual_length):
        """
        Access to actual length outside of __init__.
        """
        self.actual_length = int(actual_length)

    def __get_length__(self):
        """
        Standard access to self.length
        """
        return self.length

    def __is_index__(self):
        """
        Returns true if the type is 'Index'.
        """
        return self.type == 'Index'

    def __write_as_string__(self):
        """
        Converts the stored information into the format casava prefers.
        """
        read_type_char = 'Y'
        if self.type == 'Index':
            read_type_char = 'I'
        number_of_ns = self.length - self.actual_length
        if self.length == 0:
            number_of_ns = 0
        output = read_type_char + str(self.actual_length)
        for i in range(0,number_of_ns):
            output += 'n'
        return output

class SeqReadSet():
    """
    Read information is provided as a set.  This stores the individual read information
    and provides methods to read/write it.
    """

    def __init__(self,run_parameters_path=None):
        """
        Just an array of reads.
        """
        self.seq_reads = []
        if not run_parameters_path is None:
            self.__inject_seq_reads_from_run_parameters__(run_parameters_path)

    def __inject_seq_reads_from_run_parameters__(self,path):
        """
        Injects the reads information from the runParameters.xml file necessary for casava
        """
        tree = ET.ElementTree(file=path)
        for elem in tree.iter(tag='Read'):
            if elem.attrib['IsIndexedRead'] == 'Y':
                read_type = 'Index'
            else:
                read_type = 'Normal'
            seq_read = SeqRead(read_type,elem.attrib['NumCycles'])
            self.seq_reads.append(seq_read)
        #For the case where there is only a single index, actual length is always
        #length - 1.  This must be corrected.
        if self.__count_indices__() == 1:
            for seq_read in self.seq_reads:
                if seq_read.__is_index__():
                    seq_read.__set_actual_length__(seq_read.__get_length__() - 1)

    def __count_indices__(self):
        """
        Counts the number of seq reads of type 'Index' in the set.
        """
        count = 0
        for seq_read in self.seq_reads:
            if seq_read.__is_index__():
                count += 1
        return count

    def __write_as_string__(self):
        """
        Converts the stored information into the format casava needs.
        """
        output = []
        for seq_read in self.seq_reads:
            output.append(seq_read.__write_as_string__())
        return ",".join(output)

if __name__ == '__main__':
    #Handle arguments
    parser = argparse.ArgumentParser(description='Test various functions in this package')
    parser.add_argument('--path', dest="path", type=str, help='Provides the path for the function to be tested.')
    parser.add_argument('--index_count', dest="index_count", action="store_true", default=False, help='Tests the index count function.')
    parser.add_argument('--get_mask', dest="get_mask", action="store_true", default=False, help='Tests the get mask function.')

    args = parser.parse_args()
    seq_read_set = SeqReadSet()
    seq_read_set.__inject_seq_reads_from_run_parameters__(args.path)
    if args.index_count:
        print seq_read_set.__count_indices__()
    if args.get_mask:
        print seq_read_set.__write_as_string__()
