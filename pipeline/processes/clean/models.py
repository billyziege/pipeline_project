import os
import re
from time import strftime, localtime
from physical_objects.hiseq.models import Sample
from processes.models import SampleQsubProcess
from processes.pipeline.models import Bcbio
from template.scripts import fill_template

class CleanBcbio(SampleQsubProcess):
    """
    Runs the clean bcbio process that removes superfluous information in the
    run directory and moves the result back to storage. 
    """

    def __init__(self,config,key=int(-1),sample=None,bcbio=None,input_dir=None,base_output_dir=None,output_dir=None,date=strftime("%Y%m%d",localtime()),time=strftime("%H:%M:%S",localtime()),process_name='clean',complete_file=None,**kwargs):
        if bcbio is None:
            bcbio = Bcbio(config,key=int(-1))
        if bcbio.__class__.__name__ != "Bcbio":
            raise Exception("Trying to start a summary_stats process on a non-bcbio pipeline.")
        SampleQsubProcess.__init__(self,config,key=key,sample=sample,input_dir=input_dir,output_dir=output_dir,process_name=process_name,complete_file=complete_file,**kwargs)
        self.sample_file = bcbio.sample_file

    def __is_complete__(self,*args,*kwargs):
        return SampleQsubProcess.__is_complete__(self,*args,**kwargs)

class Clean(SampleQsubProcess):
    """
    Runs the clean process that removes superfluous information in the
    run directory and moves the result back to storage and output directory. 
    """

    def __init__(self,config,process_name='clean',**kwargs):
        SampleQsubProcess.__init__(self,config,process_name=process_name,**kwargs)

    def __is_complete__(self,*args,**kwargs):
        return SampleQsubProcess.__is_complete__(self,*args,**kwargs)
