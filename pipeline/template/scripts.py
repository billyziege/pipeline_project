import re
import sys
from mockdb.scripts import convert_attribute_value_to_array

def find_standard_fields(input_string):
    """
    Returns a list of all of the standard fields in an input string
    """
    return re.findall("FIELDBEGIN [\w,_]+ FIELDEND", input_string)

def find_array_fields(input_string):
    """
    Returns a list of all of the ARRAYVARIABLEASSIGNMENT fields in an input string
    """
    return re.findall("ARRAYVARIABLEASSIGNMENTBEGIN .* ARRAYVARIABLEASSIGNMENTEND", input_string) 

def find_task_fields(input_string):
    """
    Returns a list of all of the TASKVARIABLEASSIGNMENT fields in an input string
    """
    return re.findall("TASKVARIABLEASSIGNMENTBEGIN.*TASKVARIABLEASSIGNMENTEND", input_string,re.DOTALL)#re.DOTALL permits newlines 

def clean_demarcation(input_string,demarcation):
    """
    Removes the BEGIN and END demarcators from string.
    """
    output_string = re.sub(demarcation+"BEGIN","",input_string);
    output_string = re.sub(r"^\s+","",output_string);
    output_string = re.sub(demarcation+"END","",output_string);
    output_string = re.sub(r"\s+$","",output_string);
    return output_string

def clean_fields(fields,demarcation):
    """
    Removes all of the field demarcaters from a list of fields. 
    """
    return [clean_demarcation(f,"FIELD") for f in set(fields)]

def fill_standard_fields(input_string,dictionary):
    """
    Replaces the standard fields with the value from the dictionary
    within the input_string.
    """
    output_string = input_string
    fields = clean_standard_fields(find_standard_fields(input_string));
    for k in fields:
        output_string, number = re.subn("FIELDBEGIN " + k + " FIELDEND",dictionary[k],output_string)
    return output_string

def fill_array_fields(input_string,dictionary):
    """
    Extracts the portion of the input string demarcated by ARRAYVARIABLEASSIGNMENT,
    and serially replaces the contents of the contained field (only 1!!)
    with the array values stored within the dictionary.
    """
    output_string = input_string
    array_variables = find_array_fields(input_string)
    cleaned_contents = clean_fields(array_varaibles,"ARRAYVARIABLEASSIGNMENT")
    for cleaned_content in cleaned_contents:
        fields = clean_standard_fields(find_standard_fields(cleaned_content));
        replacement_list = []
        for field in fields: #There can only be 1.
            values = convert_attribute_value_to_array(dictionary[field]) 
            for value in values:      
                replacement_list.append(re.sub(r"ARRAYVARIABLEASSIGNMENTBEGIN\s+" + field + r"\s+ARRAYVARIABLEASSIGNMENTEND",value,cleaned_content))
        output_string = re.sub(cleaned_content," ".join(replacement_list),output_string)
    output_string, number = re.subn(r"ARRAYVARIABLEASSIGNMENTBEGIN\s+","",output_string)
    output_string, number = re.subn(r"\s+ARRAYVARIABLEASSIGNMENTBEGIN","",output_string)
    return output_string
            
def fill_task_fields(input_string,dictionary):
    """
    Extracts the portion of the input string demarcated by TASKVARIABLEASSIGNMENT,
    and serially replaces the contents of the contained field(s).
    """
    output_string = input_string
    task_variables = find_task_fields(input_string)
    cleaned_contents = clean_fields(task_varaibles,"TASKVARIABLEASSIGNMENT")
    for cleaned_content in cleaned_contents:
        fields = clean_standard_fields(find_standard_fields(cleaned_content));
        replacement_list = []
        for task_number in range(dictionary["number_tasks"]):
            dictionary["task_position_id"] = task_number
            temp_dict = {}
            for field in fields:
                values = convert_attribute_value_to_array(dictionary[field]) 
                temp_dict[field] = values[task_number]
            replacement_list.append(fill_standard_fields(cleaned_content,temp_dict))
        output_string = re.sub(cleaned_content,"\n".join(replacement_list),output_string)
    output_string, number = re.subn(r"TASKVARIABLEASSIGNMENTBEGIN\s+","",output_string)
    output_string, number = re.subn(r"\s+TASKVARIABLEASSIGNMENTBEGIN","",output_string)
    return output_string
            
def fill_template(template_file,dictionary):
    """
    Fills the template with the appropriate fields from the dictionary.
    """
    with open(template_file,'r') as f:
        template_string = f.read()
    template_string = fill_task_fields(template_string,dictionary)
    template_string = fill_array_fields(template_string,dictionary)
    template_string = fill_standard_fields(template_string,dictionary)
    return template_string


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
