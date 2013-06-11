import os
from mockdb.models import KeyedObject
# Create your models here.

class HiSeqMachine(KeyedObject):
    pass

class Flowcell(KeyedObject):
    pass

class Lane(KeyedObject):

    def __init__(self,config,flowcell=None,number='dummy_lane',*args,**kwargs):
        if flowcell is None:
            flowcell = Flowcell(config,key='dummy_flowcell_key')
        if flowcell.__class__.__name__ != 'Flowcell':
            print flowcell.__class__.__name__
            raise TypeError("A lane is trying to be assigned to a non-flowcell object.\n")
        self.key = flowcell.key + "_lane_" + number
        self.obj_type = self.__class__.__name__
        self.flowcell_key = flowcell.key
        self.number = number
        self.percentage_pf = None
        self.percentage_above_q30 = None
        self.total_reads = None
        self.undetermined_reads = None

class Barcode(KeyedObject):

    def __init__(self,config,sample=None,index='dummy',lane=None,**kwargs):
        if sample is None:
            sample = Sample(config,key="dummy_sample_key")
        if lane is None:
            lane = Lane(config,key="dummy_lane_key")
        if lane.__class__.__name__ != 'Lane':
            raise TypeError("A barcode is trying to be assigned to a non-lane object.\n")
        if sample.__class__.__name__ != 'Sample':
            raise TypeError("A barcode is trying to be assigned to a non-sample object.\n")
        self.key = lane.key + "_" + index
        self.obj_type = self.__class__.__name__
        self.sample_key = sample.key
        self.lane_key = lane.key
        self.index = index
        self.reads = None
