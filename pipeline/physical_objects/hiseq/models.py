import os
from physical_objects.models import Sample
from mockdb.models import KeyedObject
# Create your models here.

class HiSeqMachine(KeyedObject):
    pass

class Flowcell(KeyedObject):
    pass

class Lane(KeyedObject):

    def __init__(self,config,flowcell=None,number='dummy_lane',*args,**kwargs):
        if not flowcell is None:
            self.key = flowcell.key + "_lane_" + number
            self.obj_type = self.__class__.__name__
            self.flowcell_key = flowcell.key
            self.number = number
            self.percentage_pf = None
            self.percentage_above_q30 = None
            self.total_reads = None
            self.undetermined_reads = None

class Barcode(KeyedObject):

    def __init__(self,config,sample=None,project=None,index='dummy',lane=None,**kwargs):
        if not sample is None:
            self.key = lane.key + "_" + index
            self.obj_type = self.__class__.__name__
            self.sample_key = sample.key
            self.lane_key = lane.key
            self.flowcell_key = lane.flowcell_key
            self.project = project
            self.index = index
            self.reads = None
