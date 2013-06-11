import ConfigParser
from mockdb.models import KeyedObject, NumberedObject, SetOfKeyedObjects, SetOfNumberedObjects
from class_pedigree.scripts import classes_list, progenitor_classes, ancestor_classes
#This function initiates an object that contains a set of every
#object that has KeyedObejct as a progenitor
def initiate_mockdb(config):
    clses = classes_list(config)
    mockdb={}
    for cls in clses:
        pro_cs = progenitor_classes(cls)
        if len(pro_cs) > 1:
            continue
        if not KeyedObject in pro_cs:
            continue
        anc_cs = ancestor_classes(cls)
        if NumberedObject in anc_cs:
            mockdb[cls.__name__] = SetOfNumberedObjects(cls=cls)
        else:
            mockdb[cls.__name__] = SetOfKeyedObjects(cls=cls)
        mockdb[cls.__name__].__load__(config)
    return mockdb

def save_mockdb(config,mockdb):
    for class_name, set_of_objects in mockdb.iteritems():
        set_of_objects.__save__(config)
    return 1

if __name__ == '__main__':
    config = ConfigParser.ConfigParser()
    config.read('/mnt/iscsi_space/zerbeb/qc_pipeline_project/qc_pipeline/config/qc.cfg')
    initiate_mockdb(config)
