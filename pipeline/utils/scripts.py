import argparse
import ConfigParser
from mockdb.initiate_mockdb import initiate_mockdb, save_mockdb
from class_pedigree.scripts import child_classes
from physical_objects.models import Sample

def rename_all_sample_keys(mockdb,orig_sample_key,new_sample_key):
    count = 0
    for collection_key, collection in mockdb.iteritems():
        for object_key, object in collection.objects.iteritems():
            try:
                if object.sample_key == orig_sample_key:
                    object.sample_key = new_sample_key
                    count += 1
            except AttributeError:
                pass
    return count

def rename_sample_key(config,mockdb,orig_sample_key,new_sample_key):
    collection_clses = child_classes(Sample,config=config) + [Sample]
    for cls in collection_clses:
        collection = mockdb[cls.__name__]
        for object_key, object in collection.objects.iteritems():
            if object.key == orig_sample_key:
                object.key = new_sample_key
    return 1
        
if __name__ == '__main__':
    #Handle arguments
    parser = argparse.ArgumentParser(description='Puts the project-summary.csv info into the bcbio database')
    parser.add_argument('sample_key', type=str, help='The sample name as it currently is in the database')
    parser.add_argument('new_key', type=str, help='The sample name to which the sample_key will be renamed')
    args = parser.parse_args()
    config = ConfigParser.ConfigParser()
    config.read('/home/sequencing/src/pipeline_project/pipeline/config/qc_on_ihg.cfg')
    mockdb = initiate_mockdb(config)
    rename_all_sample_keys(mockdb,args.sample_key,args.new_key)
    rename_sample_key(config,mockdb,args.sample_key,args.new_key)
    save_mockdb(config,mockdb)
