
def write_list_file(input_list,output_file,original_list_file=None,sort=True):
    output_list = []
    if not original_list_file is None:
        with open(original_list_file,'r') as f:
            output_list = [line.rstrip() for line in f]
    output_list.extend(input_list)
    if sort is True:
        output_list.sort()
    with open(output_file,'w') as f:
        f.write("\n".join(output_list))
        f.write("\n")
    return 1
