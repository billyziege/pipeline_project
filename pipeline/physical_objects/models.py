import os
from mockdb.models import KeyedObject
# Create your models here.

class Sample(KeyedObject):
    """
    The basic unit of sample.
    """

    def __init__(self,config,key='dummy',state='active',plate=None,partner_key=None):
        """
        Initialize the sample object.
        """
        self.state = state
        self.current_plate = plate
        self.partner_key = partner_key #The active partnering.
        try:
            int(key[0])
            self.key = "Sample_" + str(key)
        except:
            self.key = key
        self.obj_type = self.__class__.__name__

    def change_state(self,state):
        """
        Offers a hook for changing the state.
        """
        self.state = state

    def __is_matched__(self):
        """
        Returns true if the sample is paired with another sample.
        """
        if self.partner_key is None:
            return False
        return True

class Case(Sample):
    """
    The sub-class of sample.
    """
    pass

class Control(Sample):
    """
    The sub-class of sample.
    """
    pass

class CaseControlPair(KeyedObject):
    """
    Contains the relational information of a possible case control pair.
    """

    def __init__(self,config,case=None,control=None,*args,**kwargs):
        """
        Initializes the case control pair relation object.
        """
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
