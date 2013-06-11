import os
from mockdb.models import KeyedObject
# Create your models here.

class Sample(KeyedObject):
    def __init__(self,config,key='dummy',state='active',project=None):
        self.state = state
        self.project = project
        try:
            int(key[0])
            self.key = "Sample_" + str(key)
        except:
            self.key = key
        self.obj_type = self.__class__.__name__

    def change_state(self,state):
        self.state = state

class Case(Sample):
    pass

class Control(Sample):
    pass

class CaseControlPair(KeyedObject):

    def __init__(self,config,case=None,control=None,*args,**kwargs):
        if case is None:
            case = Case(config,key='dummy_case_key')
        if control is None:
            control = Control(config,key='dummy_control_key')
        if case.__class__.__name__ != 'Case':
            raise TypeError("A non-case class was fed into the first element of the Match class.\n")
        if control.__class__.__name__ != "Control":
            raise TypeError("A non-control class was fed into the second element of the Match class.\n")
        self.key = case.key + "x" + control.key
        self.obj_type = self.__class__.__name__
        self.case_key = case.key
        self.control_key = control.key
    def __str__(self):
        return self.key
    def __unicode__(self):
        return self.key
