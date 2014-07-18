import re
import os
def upperrepl(matchobj):
    """
    Replaces an upercase matched object string 
    with an underscore and a lower case string.  This is
    used in class_to_dir
    """
    return '_' + matchobj.group(0).lower()

def class_to_dir(cls):
    """
    Defines the relationship between the class name and 
    the name of the class in lowercase file structure.
    """
    return cls.__name__[0:1].lower() + re.sub("[A-Z]",upperrepl,cls.__name__[1:])

def extract_db_keys(db_fname, sep=','):
    """
    Extracts the first line of a db, which is the header.
    """
    if not os.path.isfile(db_fname):
        return []
    with open(db_fname,'r') as f:
        keys = f.readline().rstrip().split(sep)
    if keys == None:
  	    return []
    return keys

def db_file_name(progenitor_cls, cls, base_dir):
    """
    Returns the db file name corresponding to the given class.
    """
    db_dir = base_dir + '/' + class_to_dir(progenitor_cls)
    db_fname = db_dir + '/' + class_to_dir(cls) + ".db"
    return db_fname

def translate_underscores_to_capitals(name):
    """
    Depending on the convention, a db file may be referred to as something like
    object_name or ObjectName.  This converts from the former to the latter.
    """
    pieces = name.split("_")
    capital_pieces = [x[0:1].upper() + x[1:] for x in pieces]
    return "".join(capital_pieces)

def convert_attribute_value_to_array(input_string):
    """
    Attributes may container arrays.  These have the following formats only:
      [value1:value2:value3] which is a 1 dimensional array of 3 values.
    This function converts such a string value to an appropriate nested list.
    """
    return input_string.split(":")
    

