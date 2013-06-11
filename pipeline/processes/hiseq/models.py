import os
from time import strftime, localtime
from physical_object.hiseq.models import Flowcell, HiSeqMachine
from processes.models import GenericProcess
# Create your models here.

class SequencingRun(GenericProcess):
    """
    This object manages and stores information about a sequencing run.  It is currently written to catch the results of casava and 
    to search the hiseq data directory for additional information.
    """

    def __init__(self,config,key=int(-1),flowcell=None,machine=None,date='dummy',run_number='dummy',output_dir=None,complete_file=None,side='dummy',begin_timestamp=None,end_timestamp=None,operator=None,process_name='sequencing_run',*args,**kwargs):
        """
        Initializes the object.
        """
        if flowcell is None:
            flowcell = Flowcell(config,key="dummy_flowcell_key")
        if machine is None:
            machine = HiSeqMachine(config,key="dummy_machine_key")
        GenericProcess.__init__(self,config,key=key,date=date,time=begin_timestamp,**kwargs):
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
        """
        Checks the complete file and handles any notifications.
        """
        return os.path.isfile(self.complete_file)
