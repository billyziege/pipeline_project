import re

#This function returns a list of texts bookended by
#<name> and </name>.
def grab_text_between_tags(string,name):
    if string == "":
        return []
    search_text = "<" + name + ">(.*)<\/" + name + ">"
    matches = re.findall(search_test,string)
    no_tag = [m[1] for m in matches]
    return no_tag

#This function extracts the elements from the 
#barcode lane stats table stored in the 
#file Demultiplex_Stats.htm
def extract_barcode_lane_stats(fname):
    with open(fname,"r") as f:
        content = f.read()
    cleaned, num_cleans = re.subn('</tr>','',content)
    chunks = cleaned.split("<tr>\n")   
    if not re.search("Barcode lane statistics",chunks[0]):
        raise Exception
    column_names = parse_header_row(chunks[1])
    sample_index = grab_index(column_names,'Sample ID')
    struct = []
    for i in range(2,len(chunks)):
        row_entries = parse_data_row(chunks[i]);
        if len(row_entries) != len(column_names):
            raise Exception
        d = {}
        val, number_commas = re.subn(",","",str(row_entries[sample_index]))
        for j in range(0,len(row_entries)):
            val, number_commas = re.subn(",","",str(row_entries[j]))
            d[column_names[j]] = val
        struct.extend([d])
        if re.search("</table></div>",chunks[i]):
            break
    return struct;

def grab_index(lst,name):
    for l in range(0,len(lst)):
        if lst[l] == name:
            return l
    return -1
	

#This function extracts the names of the columns into a list.
def parse_header_row(row):
    cleaned, num_cleans = re.subn("</th>\n",'',row)
    chunks = cleaned.split('<\/table>')
    elements = chunks[0].split("<th>")
    if elements[0] != "":
        raise Exception
    return elements[1:]

#This function extracts the element values into a list.
def parse_data_row(row):
    cleaned, num_cleans = re.subn("</td>\n",'',row)
    chunks = cleaned.split('<\/table>')
    elements = chunks[0].split('<td>')
    if elements[0] != "":
        raise Exception
    es = [element.strip() for element in elements[1:]]
    return es

#This function sums up all values (assumed numeric)
#for all entries called name of the same lane unless
#they are undetermined.
def calculate_lane_total(struct,name):
    total = {}
    for i in range(0,len(struct)):
        lane = struct[i]['Lane']
        sample_key = struct[i]['Sample ID']
        if sample_key == 'lane':
            continue
        val = struct[i][name]
        try:
            total[lane] += int(val)
        except KeyError:
            total[lane] = int(val)
    return total

#This function uses the % of clusters and 
#name percentage to calculate weighted name per lane.
#ie. name ='% of >= Q30 Bases (PF)' 
def calculate_weighted_percent(struct,name):
    percentage = {}
    for i in range(0,len(struct)):
        lane = struct[i]['Lane']
        sample_key = struct[i]['Sample ID']
        if sample_key == 'lane':
            continue
        try:
            p = float(struct[i]['% of raw clusters per lane'])/float(100)*float(struct[i][name])
        except ValueError:
	    p = 0	
        try:
            percentage[lane] += p
        except KeyError:
            percentage[lane] = p
    return percentage
