import datetime
import os
import pytz
import subprocess
import re
from time import strftime, localtime
from mockdb.models import NumberedObject
from physical_objects.models import Sample
from physical_objects.hiseq.models import Flowcell
from sge_queries.nodes import grab_good_node
from sge_queries.jobs import check_if_single_job_running_on_system

class GenericProcess(NumberedObject):
    """
    A generic class for any process.
    """

    def __init__(self,config,key=int(-1),date=strftime("%Y%m%d",localtime()),time=strftime("%H:%M:%S",localtime()),process_name='generic',**kwargs):
        """
        Initializes the generic process object.
        """
        NumberedObject.__init__(self,config,key=key,**kwargs)
        self.date_begin = date
        self.time_begin = time
        self.end_time = None
        self.end_date = None
        self.state = 'Initialized'
        self.process_name = process_name

    def __is_running__(self):
        """
        Checks the state of the process.  Returns true if state is identically 'Running'.
        """
        return self.state == 'Running'
    
    def __change_state__(self,new_state):
        """
        A wrapper for changing the state.  This provides a hook for later adding checks and what not when states are changed.
        """
        self.state = new_state

    def __finish__(self,date=strftime("%Y%m%d",localtime()),time=strftime("%H:%M",localtime())):
        """
        Finishes the generic process by changing the state to complete and recording the date and time.
        """
        self.__change_state__('Complete')
        self.end_time = time
        self.end_date = date

class QsubProcess(GenericProcess):
    """
    Anything process submitted via qsub requires additional information.  This class keeps track of that information
    """

    def __init__(self,config,key=int(-1),input_dir=None,base_output_dir=None,output_dir=None,date=strftime("%Y%m%d",localtime()),time=strftime("%H:%M:%S",localtime()),process_name='qsub',complete_file=None,qsub_file=None,**kwargs):
        """
        Initializes the qsub object.
        """
        GenericProcess.__init__(self,config,key=key,date=date,time=time,process_name=process_name,**kwargs)
        self.input_dir = input_dir
        if output_dir is None:
            if base_output_dir == None:
                base_output_dir = config.get('Common_directories','bcbio_upload')
            self.output_dir = base_output_dir
        else:
            self.output_dir = output_dir
        if not os.path.exists(self.output_dir) and not re.search('dummy',self.output_dir):
            os.makedirs(self.output_dir)
        self.stderr = os.path.join(self.output_dir, self.process_name + '.stderr')
        self.stdout = os.path.join(self.output_dir, self.process_name + '.stdout')
        if complete_file is None:
            self.complete_file = os.path.join(self.output_dir, self.process_name + '.complete')
        else:
            self.complete_file = complete_file
        if qsub_file is None:
            self.qsub_file = os.path.join(self.output_dir, self.process_name + '.sh')
        self.job_id = None
    
    def __launch__(self,config,qsub_file=None,node_list=None,queue_name='blades'):
        """
        Sends the job to SGE and records pertinent information.
        """
        if os.path.isfile(self.complete_file):
            os.remove(self.complete_file)
        try:
            node = grab_good_node(config,node_list=node_list)
        except Exception, msg:
            sys.stderr.write("{0}\n".format(msg))
            return False
        if qsub_file is None:
            qsub_file = self.qsub_file
        hostname = 'hostname=' + node
        qname = 'qname=' + queue_name
        command = ['qsub', '-l', hostname, '-l', qname, qsub_file]
        proc = subprocess.Popen(command,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        out = proc.stdout.read()
        expectation = "Your job ([0-9]+) \(\"" + self.qsub_file  + "\"\) has been submitted"
        self.state = 'Running'
        if re.search(expectation, out):
            self.jobid = match.group(1)
            return True
        return False

    def __is_complete__(self):
        """
        Checks to see if the complete file is created.
        """
        return os.path.isfile(self.complete_file)

    def __present_on_system__(self):
        """
        Checks to see if the job id is still listed via qstat.  This is not currently working.
        """
        return check_if_single_job_running_on_system(self.job_id)


class SampleQsubProcess(QsubProcess):
    """
    Adds the sample object to the qsub process.  This has ramifications on the output directory
    """
    def __init__(self,config,key=int(-1),sample=None,input_dir=None,base_output_dir=None,output_dir=None,date=strftime("%Y%m%d",localtime()),time=strftime("%H:%M:%S",localtime()),process_name='sample_qsub',complete_file=None,**kwargs):
        if sample is None:
            sample = Sample(config,key="dummy_sample_key")
        if sample.__class__.__name__ != "Sample":
            raise Exception("Trying to start a qcpipeline process on a non-sample.")
        self.sample_key = sample.key
        if output_dir is None:
            if base_output_dir == None:
                base_output_dir = config.get('Common_directories','bcbio_upload')
            self.output_dir = os.path.join(base_output_dir,self.sample_key + '-' + str(date))
        else:
            self.output_dir = output_dir
        QsubProcess.__init__(self,config,key=key,input_dir=input_dir,base_output_dir=base_output_dir,output_dir=self.output_dir,date=date,time=time,process_name='sample_qsub',complete_file=complete_file,**kwargs)


class QualityControlPipeline(GenericProcess):

    def __init__(self,config,key=int(-1),sample=None,flowcell=None,description=None,recipe=None,input_dir=None,base_output_dir=None,date=strftime("%Y%m%d",localtime()),time=strftime("%H:%M:%S",localtime()),process_name='qcpipeline',sequencing_run=None,running_location='Space',storage_needed=500000000,**kwargs):
        if sample is None:
            sample = Sample(config,key="dummy_sample_key")
        if sample.__class__.__name__ != "Sample":
            raise Exception("Trying to start a qcpipeline process on a non-sample.")
        if flowcell is None:
            flowcell = Flowcell(config,key="dummy_flowcell_key")
        if flowcell.__class__.__name__ != "Flowcell":
            raise Exception("Trying to start a qcpipeline process on a non-flowcell.")
        #The keys for the sub-processes in this pipeline
        self.zcat_key = None
        self.bcbio_key = None
        self.snp_stats_key = None
        self.cleaning_key = None
        #Specific information about this pipeline
        self.description = description
        self.recipe = recipe
        self.storage_needed = storage_needed
        self.input_dir = input_dir
        self.flowcell_key = flowcell.key
        self.running_location = running_location
        self.date = date
        if base_output_dir == None:
            base_output_dir = config.get('Common_directories','bcbio_upload')
        self.output_dir = os.path.join(base_output_dir, sample.key + "_" + date)
        if not os.path.exists(self.output_dir) and not re.search('dummy',sample.key):
            os.makedirs(self.output_dir)
        if sequencing_run != None:
            self.sequencing_run_key=seqencing_run.key
        else:
            self.sequencing_key=None
        GenericProcess.__init__(self,config,key=key,process_name=process_name,**kwargs)
        self.sample_key = sample.key

    def __finish__(self,storage_device,date=datetime.date.today().strftime("%Y%m%d"),time=datetime.date.today().strftime("%H:%M")):
        GenericProcess.__finish__(self,date=date,time=time)
        storage_device.my_use -= self.storage_needed

class StandardPipeline(GenericProcess):

    def __init__(self,config,key=int(-1),sample=None,flowcell=None,description=None,recipe=None,input_dir=None,base_output_dir=None,date=strftime("%Y%m%d",localtime()),time=strftime("%H:%M:%S",localtime()),process_name='qcpipeline',sequencing_run=None,running_location='Space',storage_needed=500000000,**kwargs):
        if sample is None:
            sample = Sample(config,key="dummy_sample_key")
        if sample.__class__.__name__ != "Sample":
            raise Exception("Trying to start a qcpipeline process on a non-sample.")
        if flowcell is None:
            flowcell = Flowcell(config,key="dummy_flowcell_key")
        if flowcell.__class__.__name__ != "Flowcell":
            raise Exception("Trying to start a qcpipeline process on a non-flowcell.")
        #The keys for the sub-processes in this pipeline
        self.zcat_key = None
        self.bcbio_key = None
        self.cleaning_key = None
        #Specific information about this pipeline
        self.description = description
        self.recipe = recipe
        self.storage_needed = storage_needed
        self.input_dir = input_dir
        self.flowcell_key = flowcell.key
        self.running_location = running_location
        self.date = date
        if base_output_dir == None:
            base_output_dir = config.get('Common_directories','bcbio_upload')
        self.output_dir = os.path.join(base_output_dir, sample.key + "_" + date)
        if not os.path.exists(self.output_dir) and not re.search('dummy',sample.key):
            os.makedirs(self.output_dir)
        if sequencing_run != None:
            self.sequencing_run_key=seqencing_run.key
        else:
            self.sequencing_key=None
        GenericProcess.__init__(self,config,key=key,process_name=process_name,**kwargs)
        self.sample_key = sample.key

    def __finish__(self,storage_device,date=datetime.date.today().strftime("%Y%m%d"),time=datetime.date.today().strftime("%H:%M")):
        GenericProcess.__finish__(self,date=date,time=time)
        storage_device.my_use -= self.storage_needed

