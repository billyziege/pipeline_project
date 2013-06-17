import os
import re
import sys
import ConfigParser

#This function returns the list of all classes at the top of
#the class pedigree for the given class.
def progenitor_classes(cls):
     c = list(cls.__bases__)
     if len(c) == 0:
         return [cls]
     d = []
     for base in c:
         #c.extend(classlookup(base))
         #c.extend(classlookup(base))
         #if re.search("^__main__",unicode(base)):
         #   d.extend([base])
         #elif re.search("^<class",unicode(base)):
         if re.search("^<type",unicode(base)):
             continue
         d.extend(progenitor_classes(base))
     if len(d) == 0:
         return [cls]
     return list(set(d))

#This function lists all of the base classes from
#which a class has inherited anything.
def ancestor_classes(cls):
    c = []
    bases = list(cls.__bases__)
    for base in bases:
        c.extend([base])
        c.extend(ancestor_classes(base))
    return list(set(c))

#This function lists all of the classes that inherit
#stuff from the given class.
def child_classes(cls, config=None, clses=[]):
    child = []
    if len(clses) == 0:
        if config == None:
            raise Exception
        clses = classes_list(config)
    for c in clses:
        if c != cls:
            c_anc = ancestor_classes(c)
            if cls in c_anc:
                child.extend([c])
    return child

#This function returns a list of all classes stored in CLASS_DIRs
def classes_list(config):
    """
    Lists classes stored in the the models.py files
    stored in the class dirs parameter.
    """
    py_scripts = []
    for cls_dir in config.get('Common_directories','classes').split(","):
        files = [os.path.join(cls_dir,fname) for fname in os.listdir(cls_dir) if os.path.isfile(os.path.join(cls_dir,fname))]
        py_scripts.extend([fname for fname in files if re.search("models.py$",fname)])
    names = {}
    for fname in py_scripts:
        module_front = re.sub('/','.',re.sub(config.get('Common_directories','program') + "/",'',re.sub("/models.py$","",fname)))
        with open(fname, 'r') as f:
            ms = re.finditer("\nclass (\w+)",f.read())
            for m in ms:
                if m:
                    names.update({ m.group(1): module_front + '.models' })
    clses = []
    for class_name, module_name in names.iteritems():
        module = my_import(module_name)
        c = getattr(module, class_name)
        clses.extend([c])
    return clses

def my_import(name):
    mod = __import__(name)
    components = name.split('.')
    for comp in components[1:]:
        mod = getattr(mod, comp)
    return mod

if __name__ == '__main__':
    config = ConfigParser.ConfigParser()
    config.read('/mnt/iscsi_space/zerbeb/qc_pipeline_project/qc_pipeline/config/qc.cfg')
    if len(sys.argv) < 2:
        pass
    elif sys.argv[1] == 'ancestor_classes':
        clses = classes_list(config)
        for cls in clses:
             if re.search(sys.argv[2],cls.__name__):
                 print str(cls) + ":" + ",".join([c.__name__ for c in ancestor_classes(cls)])
