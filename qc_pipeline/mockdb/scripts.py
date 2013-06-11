import re
import os
#This function replaces an upercase matched object string 
#with an underscore and a lower case string.  This is
#used in class_to_dir
def upperrepl(matchobj):
    return '_' + matchobj.group(0).lower()

#This function defines the relationship between the class name and 
#the name of the class in lowercase file structure.
def class_to_dir(cls):
    return cls.__name__[0:1].lower() + re.sub("[A-Z]",upperrepl,cls.__name__[1:])

#The first line of a db contains the key information
def extract_db_keys(db_fname, sep=','):
   if not os.path.isfile(db_fname):
       return []
   with open(db_fname,'r') as f:
       keys = f.readline().rstrip().split(sep)
   if keys == None:
	return []
   return keys

#This function returns the db file name corresponding to the given clas.
def db_file_name(progenitor_cls, cls, base_dir):
    db_dir = base_dir + '/' + class_to_dir(progenitor_cls)
    db_fname = db_dir + '/' + class_to_dir(cls) + ".db"
    return db_fname

