import os
import sys
import datetime
import inspect
import re

from class_pedigree.scripts import progenitor_classes, ancestor_classes, child_classes, classes_list
from mockdb.scripts import db_file_name, extract_db_keys
#from django.utils import timezone
#from django.db import models
#from your_project import settings
#from msbp import settings
#from django.core.management import setup_environ
#setup_environ(settings)

# Create your models here.

class FormattingError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)
    def __unicode__(self):
        return repr(self.value)

class KeyedObject:

    def __init__(self,config,key='dummy',*args,**kwargs):
        self.key = key
        self.obj_type = self.__class__.__name__
        #for k, v in kwargs.iteritems():
        #    setattr(self,k,v)

    def __str__(self):
        return self.key

    def __unicode__(self):
        return self.key

    def __eq__(self,other):
        for attr in self.__dict__():
            try:
                if getattr(self,attr) != getattr(other,attr):
                    return False
            except AttributeError:
                return False
        return True

    def __ne__(self,other):
        if self.__eq__(self,other):
           return False
        return True

    def __contents_as_string__(self,config,cls=None,base_dir=None,keys=None,sep=','):
        if base_dir == None:
            base_dir = config.get('Common_directories','mockdb')
        if cls == None:
            cls = self.__class__
        if keys == None:
            db_fname = db_file_name(KeyedObject,cls,base_dir)
            if os.path.isfile(db_fname):
                ks = extract_db_keys(db_fname)
                if ks == []:
                    ks.extend(self.__dict__.keys().sort())
                #self_ks = self.__dict__.keys()
                #if set(self_keys).issubset(set(keys)):
                #    pass
                #else:
                #    raise FormattingError("Keys are not a subset of attibutes.")
            else:
                ks = self.__dict__.keys()
                ks.sort()
        else:
             ks = list(keys)
        string = ''
        ks.remove('key')
        ks = ['key'] + list(ks)
        header = sep.join(ks)
        for k in ks:
            if string != '':
                string += ','
            try:
                string += str(getattr(self,k))
            except AttributeError:
                string += 'None'
        return header, string

class NumberedObject(KeyedObject):

    def __init__(self,config,key=int(-1),*args,**kwargs):
        self.obj_type = self.__class__.__name__
        try:
            int(key)
            self.key = key
        except:
            raise ValueError("Trying to use the key " + key + " for a numbered-key object.")
        #for k, v in kwargs.iteritems():
            #if k == 'key':
            #setattr(self,k,v)
    
class SetOfKeyedObjects:

    def __init__(self,cls=KeyedObject,*args,**kwargs):
        self.objects={}
        self.cls=cls
        for k, v in kwargs.iteritems():
            setattr(self,k,v)

    def __key_set__(self,cls):
        key_set = set([])
        for key, instance in self.objects.iteritems():
            if instance.__class__ != cls:
                continue
            keys = instance.__dict__.keys()
            key_set.update(set(keys))
        return key_set

    def __is_contained__(self,other,keys=None):
        if keys == None:
            keys = list(self.__key_set__(self.cls))
        for key in keys:
            try:
                if self.objects[key] != other.objects[key]:
                    return False
            except KeyError:
                return False
        return True

    def __class_set__(self):
        clses = set([])
        for key, instance in self.objects.iteritems():
            cls = instance.__class__
            clses.update(set([cls]))
        return clses
            
    def __attribute_value_to_key_dict__(self,attr):
        vals = []
        d = {}
        for i in self.objects.keys():
            val = getattr(self.objects[i],attr)
            if val not in vals:
                d[val]=[]
                vals.extend([val])
            d[val].append(i)
        return d

    def __attribute_value_to_object_dict__(self,attr):
        vals = []
        d = {}
        for i in self.objects.keys():
            val = getattr(self.objects[i],attr)
            if val not in vals:
                d[val]=[]
                vals.extend([val])
            d[val].append(self.objects[i])
        return d

    def __load__(self,config,no_children = True, base_dir = None, key=None):
        if base_dir == None:
            base_dir = config.get('Common_directories','mockdb')
        first_clses =  progenitor_classes(self.cls)
        if len(first_clses) > 1:
            raise TypeError("The class {0} cannot be made into a SetOf class.\n".format(cls.__name__))
        #Some keyed classes might have children classes.  Load these as well unless no_children is flagged
        if no_children is True:
            clses = [self.cls]
        else:
            clses = [self.cls] + child_classes(self.cls,config=config)
        for c in clses:
            db_fname = db_file_name(first_clses[0],self.cls,base_dir)
            #sys.stderr.write("%s\n" % db_fname)
            if os.path.isfile(db_fname):
                keys = extract_db_keys(db_fname)
                key_index=keys.index('key')
                with open(db_fname,'r') as f:
                    f.readline() #Gets past header to contents.
                    for line in f:
                        vals = line.rstrip().split(',')
                        if len(vals) != len(keys):
                            raise FormattingError("The formatting in {0} is incorrect.  The number of keys and the number of values are different.\n".format(db_fname))
                        instance = self.cls(config)
                        for i in range(0,len(vals)):
                            if vals[i] == 'None':
                                setattr(instance,keys[i],None)
                                continue
                            if vals[i] == 'True':
                                setattr(instance,keys[i],True)
                                continue
                            if vals[i] == 'False':
                                setattr(instance,keys[i],False)
                                continue
                            if keys[i] == 'sample_key' or self.cls.__name__ == 'Sample':
                                setattr(instance,keys[i],vals[i])
                                continue
                            try:
                                int_val = int(vals[i])
                                setattr(instance,keys[i],int_val)
                            except ValueError:
                                setattr(instance,keys[i],vals[i])
                        if key != None:
                            if instance.key not in [key]:
                                continue
                        #Load all keys if key is none.  Elsewise, just that key.
                        self.objects[instance.key] = instance

#    def __load__(self,no_children = True, base_dir = BASE_DB_DIR, key=None):
#        first_clses =  progenitor_classes(self.cls)
#        if len(first_clses) > 1:
#            raise TypeError("The class {0} cannot be made into a SetOf class.\n".format(cls.__name__))
#        #Some keyed classes might have children classes.  Load these as well unless no_children is flagged
#        if no_children == True:
#            clses = [self.cls]
#        else:
#            clses = [self.cls] + child_classes(self.cls)
#        for c in clses:
#            db_fname = db_file_name(first_clses[0],self.cls,base_dir)
#            if os.path.isfile(db_fname):
#                keys = extract_db_keys(db_fname)
#                with open(db_fname,'r') as f:
#                    f.readline() #Gets past header to contents.
#                    for line in f:
#                	#if self.cls.__name__=='SequencingRun':
#			#    print line
#                        vals = line.rstrip().split(',')
#                        if len(vals) != len(keys):
#                            raise FormattingError("The formatting in {0} is incorrect.  The number of keys and the number of values are different.\n".format(db_fname))
#                        d={}
#                        for i in range(0,len(vals)):
#                            if vals[i] == 'None':
#                                d[keys[i]] = None
#                                continue
#                            d[keys[i]] = vals[i]
#                        #Load all keys if key is none.  Elsewise, just the specifed key(s).
#                        if key != None:
#                            if d['key'] not in [f for f in map(str,key)]:
#                                continue
#                        instance = self.cls()
#                        for k in keys:
#                            setattr(instance,k,d[k])
#                        self.objects[d['key']] = instance

    def __get__(self,config,key,base_dir=None,**kwargs):
        if base_dir == None:
            base_dir = config.get('Common_directories','mockdb')
        try:
            return self.objects[key]
        except KeyError:
            other = self.__class__(cls=self.cls)
            other.__load__(config,base_dir=base_dir,key=[key])
            try:
                instance = other.objects[key]
            except KeyError:
                instance = self.cls(config,key=key,**kwargs)
		self.objects[key]=instance
        return instance

    def __save__(self,config,base_dir=None):
        if base_dir == None:
            base_dir = config.get('Common_directories','mockdb')
        if self.objects.keys() == []:
            return 1
        obj_lines = {}
        header = {}
        clses = self.__class_set__()
        for cls in clses:
            obj_lines[cls] = {}
            header[cls] = ""
        for key, instance in self.objects.iteritems():
            #try:
            cls = instance.__class__
            head, obj_lines[cls][key] = instance.__contents_as_string__(config,base_dir=base_dir)
            if header[cls] == "":
                header[cls] = head
            if header[cls] != head:
                raise FormattingError("Different objects of the same class have different keys.")
            #except FormattingError:
            #    self.__overwrite__(base_dir=base_dir)
        for cls in obj_lines.keys():
            db_fname = db_file_name(KeyedObject, cls, base_dir)
            if not os.path.isfile(db_fname):
                with open(db_fname,'w') as f:
                    f.write(header[cls] + "\n")
                    for key in obj_lines[cls].keys():
                        f.write(obj_lines[cls][key] + "\n")
                continue
            db_ids = set(extract_db_keys(db_fname))
            obj_ids = set(obj_lines[cls].keys())
            intersection = db_ids.intersection(obj_ids)
            other = self.__class__(self.cls)
            other.__load__(config,key=intersection)
            if self.__is_contained__(other,*intersection):
                with open(db_fname,'a') as f:
                    f.write(header[cls] + "\n")
                    for key in obj_lines[cls].keys():
                        f.write(obj_lines[cls][key] + "\n")
            else:
                self.__overwrite__(config,base_dir=base_dir)
        return 1
     
    def __overwrite__(self,config,base_dir=None,begin_fresh=False):
        if base_dir == None:
            base_dir = config.get('Common_directories','mockdb')
        clses = self.__class_set__()
        for cls in clses:
            if begin_fresh == True:
                union_ids = set(self.objects.keys())
            else:
                stored_db = self.__class__(cls=cls)
                stored_db.__load__(config)
                stored_ids = set(stored_db.objects.keys())
                current_ids = set(self.objects.keys())
                union_ids = list(current_ids.union(stored_ids))
                stored_keys = stored_db.__key_set__(cls)
                current_keys = self.__key_set__(cls)
                union_keys = list(current_keys.union(stored_keys))
            out_keys = list(union_keys)
            out_keys.remove('key')
            union_ids.sort()
            out_keys.sort()
            out_keys = ['key'] + out_keys
            db_fname = db_file_name(KeyedObject, cls, base_dir)
            with open(db_fname,'w') as f:
                f.write(",".join(out_keys) + "\n")
                for i in union_ids:
                    if i in current_ids:
                        header, string = self.objects[i].__contents_as_string__(config,base_dir=base_dir,cls=cls,keys=out_keys)
                    else:
                        header, string = stored_db.objects[i].__contents_as_string__(config,base_dir=base_dir,cls=cls,keys=out_keys)
                    f.write(string + "\n")

    def __combine__(self,other):
        """
        Adds the entries with the appropriate classes from other
        into self.  If there all entries of other are from a class
        that self does not contain, doesn't do anything.
        """
        clses = self.__class_set__()
        for cls in clses:
            current_ids = set([cid for cid in self.objects.keys() if self.objects[cid].obj_type == cls.__name__])
            other_ids = set([cid for cid in self.objects.keys() if self.objects[cid].obj_type == cls.__name__])
            for oid in other_ids:
                if oid in current_ids:
                    continue
                self.objects.update({oid: other.objects[oid]})

    def __remove_entry__(self,config,key):
        """
        Loads all entries within the db file into self,
        removes the entry with the provided key, and then
        writes all the information back to the database.
        """
        stored_db = self.__class__(cls=cls)
        stored_db.__load__(config)
        self.__combine__(stored_db)
        del(self.objects[key])
        self.overwrite(config,begin_fresh=True)
        

class SetOfNumberedObjects(SetOfKeyedObjects):

    def __init__(self,cls=NumberedObject,*args,**kwargs):
        SetOfKeyedObjects.__init__(self,cls=cls,*args,**kwargs)

    def __max_key__(self,config):
        keys = self.objects.keys()
        #Load database and children databases to get 
        #access to their keys
        other = self.__class__(cls=self.cls)
        other.__load__(config,no_children=False)
        keys += other.objects.keys()
        anc_clses = ancestor_classes(self.cls)
        #Load ancestor databases to get 
        #access to their keys
        for cls in anc_clses:
            other = SetOfNumberedObjects(cls=cls)
            other.__load__(config)
            keys += other.objects.keys()
        if keys == []:
            return 0
        keys.sort(key=int)
        return keys[-1]

    def __new__(self,config,*args,**kwargs):
        new_key = int(self.__max_key__(config)) + 1
        #init_keys = inspect.getargspec(self.cls.__init__)[0]
        instance = self.cls(config,key=int(new_key),*args,**kwargs)
        self.objects.update({new_key:instance})
        return instance

    def __get__(self,config,key,base_dir=None,**kwargs):
        if base_dir == None:
            base_dir = config.get('Common_directories','mockdb')
        k = int(key)
        try:
            return self.objects[k]
        except KeyError:
            other = self.__class__(cls=self.cls)
            other.__load__(config,base_dir=base_dir,key=key)
            try:
                instance = other.objects[int(key)]
            except KeyError:
                instance = self.cls(config,key=k,**kwargs)
		self.objects[k]=instance
        return instance

    def __str__(self):
        return str(self.key)
     
    def __save__(self,config,base_dir=None):
        if base_dir == None:
            base_dir = config.get('Common_directories','mockdb')
        if self.objects.keys() == []:
            return 1
        obj_lines = {}
        header = {}
        clses = self.__class_set__()
        for cls in clses:
            obj_lines[cls] = {}
            header[cls] = ""
        for key, instance in self.objects.iteritems():
            #try:
            cls = instance.__class__
            head, obj_lines[cls][key] = instance.__contents_as_string__(config,base_dir=base_dir)
            if header[cls] == "":
                header[cls] = head
            if header[cls] != head:
                raise FormattingError("Different objects of the same class have different keys.")
            #except FormattingError:
            #    self.__overwrite__(base_dir=base_dir)
        for cls in obj_lines.keys():
            db_fname = db_file_name(KeyedObject, cls, base_dir)
            if not os.path.isfile(db_fname):
                with open(db_fname,'w') as f:
                    f.write(header[cls] + "\n")
                    for key in obj_lines[cls].keys():
                        f.write(obj_lines[cls][key] + "\n")
                continue
            #Can get rid of this save function (inheritance) if I instead write extract_db_keys for SetOfKeyedObject and SetOfNumberObject 
            db_ids = set(extract_db_keys(db_fname))
            obj_ids = set(obj_lines[cls].keys())
            intersection = db_ids.intersection(obj_ids)
            other = self.__class__(self.cls)
            other.__load__(config,key=intersection)
            if self.__is_contained__(other,*intersection):
                with open(db_fname,'a') as f:
                    f.write(header[cls] + "\n")
                    for key in obj_lines[cls].keys():
                        f.write(obj_lines[cls][key] + "\n")
            else:
                self.__overwrite__(config,base_dir=base_dir)
        return 1
     
