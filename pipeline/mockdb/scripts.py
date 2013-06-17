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
    Extracts the first line of a db, which the header.
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

