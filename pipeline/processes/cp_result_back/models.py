import os
import re
from time import strftime, localtime
from physical_objects.hiseq.models import Sample
from processes.models import SampleQsubProcess
from template.scripts import fill_template

class CpResultBack(SampleQsubProcess):
    """
    Runs the cp that uploads the bcbio_upload directory to the proper location.
    """

    def __init__(self,config,key=int(-1),sample=None,input_dir=None,output_dir=None,base_output_dir=None,output_subdir='ngv3',date=strftime("%Y%m%d",localtime()),time=strftime("%H:%M:%S",localtime()),process_name='cp',complete_file=None,**kwargs):
        if base_output_dir is not None:
          cp_dir = os.path.join(base_output_dir,output_subdir)
          if not os.path.exists(cp_dir):
            os.makedirs(cp_dir)
        else:
          cp_dir = None
        self.cp_dir = cp_dir
        if sample is not None:
          self.md5_file = os.path.join(cp_dir,sample.key + "_exome_md5checksums.txt")
        else:
          self.md5_file = "exome_md5checksums.txt"
        SampleQsubProcess.__init__(self,config,key=key,sample=sample,input_dir=input_dir,output_dir=output_dir,process_name=process_name,complete_file=complete_file,**kwargs)

    def __is_complete__(self,configs=None):
        return SampleQsubProcess.__is_complete__(self)
