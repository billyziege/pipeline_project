import os
import sys
import re
import yaml
import csv

def get_genome_ref(sample_file,system_file):
    sample_yaml = grab_yaml(sample_file)
    system_yaml = grab_yaml(system_file)
    try:
        galaxy_dir = os.path.dirname(system_yaml["galaxy_config"])
        tool_dir = os.path.join(galaxy_dir,'tool-data')
        if ("algorithm" in sample_yaml and "aligner" in sample_yaml["algorithm"] and not sample_yaml["algorithm"]["aligner"] is False):
            aligner = sample_yaml["algorithm"]["aligner"]
        else:
            aligner = "bwa"
        loc_file = os.path.join(tool_dir,aligner+"_index.loc")
        genome_build = sample_yaml["details"][0]["genome_build"]
        return get_genome_ref_from_loc_file(genome_build,loc_file)
    except:
        return None

def grab_yaml(path):
    """
    Grabs all the information for the
    samples in the project_summary.yaml file, cleans it,
    and returns it as an array.
    """
    if not os.path.isfile(path):
        return None
    with open(path,'r') as f:
        return yaml.load(f)

def read_galaxy_loc_file(path):
    """
    Grabs the information from the galaxy loc file.  [1]
    is the genome build and [3] is the genome fasta file
    """
    if not os.path.isfile(path):
        raise Exception("No such file: {0}\n".format(path))
    loc_contents = []
    with open(path,'r') as f:
        reader = csv.reader(f, delimiter="\t")
        for row in reader:
            if len(row) != 4:
                continue
            if row[0].startswith('#'):
                continue
            loc_contents.append(row)
    return loc_contents
            
def find_genome_build_in_loc_file(genome_build,loc_file):
    """
    Returns the first row with the genome build from the
    [1] (second) position in the galaxy location file.
    """
    loc_contents = read_galaxy_loc_file(loc_file)
    for row in loc_contents:
        if row[1] == genome_build:
            return row
    return None

def get_genome_ref_from_loc_file(genome_build,loc_file):
    """
    Gets the genome reference from the galaxy location file.
    """
    row = find_genome_build_in_loc_file(genome_build,loc_file)
    if not row is None:
        return row[3]
    else:
        sys.stderr.write("No reference was found for "+genome_build+"\n")
        return None

