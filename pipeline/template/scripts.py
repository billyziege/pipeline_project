import re
import sys
from mako.template import Template

#This function returns the fields in a mako-formated template.
def find_fields(template_string):
    return re.findall("FIELDBEGIN [\w,_]+ FIELDEND", template_string)

#This function fills a template string with everything in an object it can.
def fill_template(template_file,dictionary):
    with open(template_file,'r') as f:
        template_string = f.read()
    string_iteration = [template_string]
    i = 0 
    fields = find_fields(template_string)#The fields begin with FIELDBEGIN and end with FIELDEND with the key in the middle.
    fields_minus_begin = [re.sub("FIELDBEGIN ","",f) for f in set(fields)]#Now with only FIELDEND and key.
    fields_minus_both = [re.sub(" FIELDEND","",f) for f in fields_minus_begin]#Now just the key.
    if False: #Set to True for debugging
        print dictionary
        print fields_minus_both
        print set(dictionary.keys()).issuperset(set(fields_minus_both))
        print set(fields_minus_both).difference(set(dictionary.keys()))
        print set(fields_minus_both)
        sys.exit()
    for k in fields_minus_both:
        string, number = re.subn("FIELDBEGIN " + k + " FIELDEND",dictionary[k],string_iteration[i])
        i += 1
        string_iteration.extend([string])
    return string
