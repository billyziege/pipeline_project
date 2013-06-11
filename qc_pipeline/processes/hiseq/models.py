import os
from time import strftime, localtime
from mockdb.models import KeyedObject,NumberedObject
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

#This is a relationship and might belong elsewhere.
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

class SequencingRun(NumberedObject):

    def __init__(self,config,key=int(-1),flowcell=None,machine=None,date='dummy',run_number='dummy',output_dir=None,complete_file=None,side='dummy',begin_timestamp=None,end_timestamp=None,operator=None,*args,**kwargs):
        if flowcell is None:
            flowcell = Flowcell(config,key="dummy_flowcell_key")
        if machine is None:
            machine = HiSeqMachine(config,key="dummy_machine_key")
        NumberedObject.__init__(self,config,key,*args,**kwargs)
        self.begin_timestamp = begin_timestamp
        self.end_timestamp = end_timestamp
        self.flowcell_key = flowcell.key
        self.machine_key = machine.key
        self.date = date
        self.run_number = run_number
        self.side = side
        self.state = "Running"
        self.operator = operator
        #self.input_amount = input_amount
        #self.yield_from_library = yield_from_library
        #self.average_bp = average_bp
        if output_dir == None:
            output_name = date + "_" + machine.key + "_" + run_number + "_" + side + flowcell.key
            self.output_dir = os.path.join(config.get('Common_directories','casava_output'),output_name)
        else:
            self.output_dir = output_dir
        if complete_file == None:
            self.complete_file = os.path.join(self.output_dir,config.get('Filenames','casava_finished'))
        else:
            self.complete_file = complete_file

    def __is_complete__(self):
        return os.path.isfile(self.complete_file)

    def __finish__(self,date=strftime("%Y%m%d",localtime()),time=strftime("%H:%M",localtime())):
        self.state = 'Complete'
        self.end_time = time
        self.end_date = date
