import datetime
import os
import pytz
import subprocess
import re
from time import strftime, localtime
from mockdb.models import NumberedObject
from physical_objects.hiseq.models import Flowcell
from processes.hiseq.models import SequencingRun
from sge_queries.nodes import grab_good_node
from sge_queries.jobs import check_if_single_job_running_on_system

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

class FlowcellStatisticsReports(GenericProcess):
    """
    Object to keep track of samples that come out of a flowcell
    from a sequencing run.  This object automatically sends reports
    when 1, 4, and 16 (and also 32 and 64 for HighThroughputRun) 
    samples have completed.
    """
    def __init__(self,config,key=int(-1),flowcell=None,seq_run=None,base_output_dir=None,process_name='flowcell_reports',**kwargs):
        if flowcell is None:
            flowcell = Flowcell(config,key="dummy_flowcell_key")
        if flowcell.__class__.__name__ != "Flowcell":
            raise Exception("Trying to start a flowcell statistics reports object on a non-flowcell.")
        if seq_run is None:
            pipeline = SequencingRun(config,key="dummy_seq_run_key")
        GenericProcess.__init__(self,config,key=key,process_name=process_name,**kwargs)
        if base_output_dir == None:
            base_output_dir = config.get('Common_directories','flowcell_reports')
        else:
            self.base_output_dir = base_output_dir
        self.flowcell_key = flowcell.key
        self.sequencing_run_key = seq_run.key
        self.pipeline_keys = None
        self.first_flowcell_report_key = None
        self.second_flowcell_report_key = None
        self.third_flowcell_report_key = None
        self.fourth_flowcell_report_key = None
        self.fifth_flowcell_report_key = None

#class FlowcellStatisticReport(QsubProcess):


