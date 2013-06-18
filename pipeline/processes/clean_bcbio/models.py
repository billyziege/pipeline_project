import os
import re
from physical_objects.hiseq.models import Sample
from processes.models import SampleQsubProcess
from template.scripts import fill_template

class CleanBcbio(SampleQsubProcess):
    """
    Runs the clean bcbio process that removes superfluous information in the
    run directory and moves the result back to storage. 
    """

    def __init__(self,config,key=int(-1),sample=None,input_dir=None,base_output_dir=None,output_dir=None,date=strftime("%Y%m%d",localtime()),time=strftime("%H:%M:%S",localtime()),process_name='clean',complete_file=None,**kwargs):
        SampleQsubProcess.__init__(self,config,key=key,sample=sample,input_dir=input_dir,output_dir=output_dir,process_name=process_name,complete_file=complete_file,**kwargs)
