import sys
import os
import re
import ast

def quick_process_re_stats(sample_key,data):
    header = ["Sample","Total","Aligned","Pair duplicates",
              "Insert size","On target bases",
              "Mean target coverage","10x coverage targets",
              "Zero coverage targets","Total variations","In dbSNP",
              "Transition/Transversion (all)",
              "Transition/Transversion (dbSNP)",
              "Transition/Transversion (novel)"]
    dictionary = {"Sample": sample_key}
    firsts = ["Total","Mean target coverage","10x coverage targets","Zero coverage targets",
               "Total variations", 'In dbSNP', 'Transition/Transversion (all)', 
               'Transition/Transversion (dbSNP)', 'Transition/Transversion (novel)']
    seconds = ["Aligned","Pair duplicates","On target bases"]
    boths = ["Insert size"]
    for entry in data:
        if entry[0] in firsts:
            dictionary.update({entry[0]: re.sub(r'%',"",str(entry[1]).strip('(').strip(')').replace(',','')).strip("\\")})
        if entry[0] in seconds:
            dictionary.update({entry[0]: re.sub(r'%',"",str(entry[2]).strip('(').strip(')').replace(',','')).strip("\\")})
        if entry[0] in boths:
            dictionary.update({entry[0]: str(entry[1]).replace(',','') + ' ' + str(entry[2]).replace(',','')})
    line = []
    for column in header:
        line.append(dictionary[column])
    print ",".join(header)
    print ",".join(line)

if __name__ == '__main__':
    sample_key = sys.argv[1]
    list_file = sys.argv[2]
    with open(list_file,'r') as f:
        data = ast.literal_eval(f.read().rstrip())
    quick_process_re_stats(sample_key,data)

