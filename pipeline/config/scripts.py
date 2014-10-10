from ConfigParser import *

class MyConfigParser(ConfigParser):
    """
    Extends config parser to do some additional stuff.
    """

    def __init__(self):
        ConfigParser.__init__(self)

    def safe_get(self,section,key,default_value=None):
        """
        Return either the value of the parameter or None if there is an exception.
        """
        try:
             return config.get(section,key)
        except:
             return default_value
    
def get_location_dictionary_from_config(config):
    location_options = config.get('Location_options','list').split(",")
    location_dirs = {}
    for location in location_options:
        directory = config.get(location + '_directories','dir')
        location_dirs.update({location:directory})
    return location_dirs

def grab_thresholds_from_config(config,section,key):
    """
    Splits the config tuple low,high that is stored
    as a string in the config file.  If either
    low or high are empty strings, the value
    is interpretted as None and returned.  
    """
    low, high = config.get(section,key).split(',')
    if low == '':
        low = None
    if high == '':
        high = None
    return low, high
