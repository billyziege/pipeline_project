import os
from mockdb.models import KeyedObject,SetOfKeyedObjects,NumberedObject,SetOfNumberedObjects
from mockdb.scripts import class_to_dir, extract_db_keys
CLASS_DIRS = ['/home/zerbeb/homemade_programs/msbp_no_django/mockdb']
def test_class_to_dir():
    class TestClass: pass
    k = TestClass()
    if class_to_dir(k.__class__) != 'test_class': print "class_to_dir failed"

def test_extract_db_keys():
    db_file=os.path.join(os.getcwd(),'mockdb/test_data/keyed_object/test_load_class.db')
    keys = extract_db_keys(db_file)
    correct_keys = ['key', 'obj_type', 'attr1', 'attr2']
    if set(keys) != set(correct_keys):
        print 'extract_db_keys failed.'

def test_load_set():
    class TestLoadClass(KeyedObject):
        def __init__(self,key='dummy',attr1='a1',attr2='a2'):
            KeyedObject.__init__(self,key=key)
            self.attr1 = attr1
            self.attr2 = attr2

    set_of_keyed_objects = SetOfKeyedObjects(TestLoadClass)
    set_of_keyed_objects.__load__(base_dir=os.path.join(os.getcwd(),'mockdb/test_data'),no_children=True,key=1)
    try:
        if set_of_keyed_objects.objects['1'].attr1 != 'attribute1':
            print 'load did not load the correct attributes.'
    except KeyError:
        print 'Specifying the key in load did not work.'
    try:
            if set_of_keyed_objects.objects['2'].attr1 != 'attribute1':
                "More keys than specified were loaded."
    except KeyError:
            pass

def test_save():
    class TestLoadClass(KeyedObject):
        def __init__(self,key='dummy',attr1='a1',attr2='a2'):
            KeyedObject.__init__(self,key=key)
            self.attr1 = attr1
            self.attr2 = attr2

    set_of_keyed_objects = SetOfKeyedObjects(TestLoadClass)
    instance = set_of_keyed_objects.cls(key='2')
    set_of_keyed_objects.objects['2'] = instance
    set_of_keyed_objects.__save__(base_dir=os.path.join(os.getcwd(),'mockdb/test_data'))

def test_class_set_method():
    class TestLoadClass(KeyedObject):
        def __init__(self,key='dummy',attr1='a1',attr2='a2'):
            KeyedObject.__init__(self,key=key)
            self.attr1 = attr1
            self.attr2 = attr2

    set_of_keyed_objects = SetOfKeyedObjects(TestLoadClass)
    instance = set_of_keyed_objects.cls(key='2')
    set_of_keyed_objects.objects['2'] = instance
    clses = set_of_keyed_objects.__class_set__()
    if clses != set([set_of_keyed_objects.cls]):
        print "class_set_method failed."


def test_contents_as_string():
    class TestLoadClass(KeyedObject):
        def __init__(self,key='dummy',attr1='a1',attr2='a2'):
            KeyedObject.__init__(self, key=key)
            self.attr1 = attr1
            self.attr2 = attr2

    ko = TestLoadClass(key='2', attr1='one',attr2='two')
    string = ko.__contents_as_string__(base_dir=os.path.join(os.getcwd(),'mockdb/test_data'))
    if string != '2,TestLoadClass,one,two':
        print "contents_as_string failed."
        print "\t" + string

def test_retrieve_values_by_key_method():
    class TestLoadClass(KeyedObject):
        def __init__(self,key='dummy',attr1='a1',attr2='a2'):
            KeyedObject.__init__(self,key=key)
            self.attr1 = attr1
            self.attr2 = attr2

    set_of_keyed_objects = SetOfKeyedObjects(TestLoadClass)
    set_of_keyed_objects.__load__(base_dir=os.path.join(os.getcwd(),'mockdb/test_data'),no_children=True)
    d = set_of_keyed_objects.__retrieve_values_by_key__('attr1')
    #print d

def test_max_key_method():
    class TestLoadClass(NumberedObject):
        def __init__(self,key=0,attr1='a1',attr2='a2'):
            KeyedObject.__init__(self,key=key)
            self.attr1 = attr1
            self.attr2 = attr2

    set_of_keyed_objects = SetOfNumberedObjects(TestLoadClass)
    set_of_keyed_objects.__load__(base_dir=os.path.join(os.getcwd(),'mockdb/test_data'),no_children=True)
    print set_of_keyed_objects.__max_key__()

test_class_to_dir()
test_load_set()
test_save()
test_class_set_method()
test_extract_db_keys()
test_contents_as_string()
test_retrieve_values_by_key_method()
test_max_key_method()
