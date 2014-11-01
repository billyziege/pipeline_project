import os
import re
from config.scripts import MyConfigParser
from time import strftime, localtime
from physical_objects.hiseq.models import Sample
from processes.models import SampleQsubProcess
from template.scripts import fill_template


class CpResultBack(SampleQsubProcess):
    """
    Runs the cp that uploads the bcbio_upload directory to the proper location.
    """

    def __init__(self,config,key=int(-1),pipeline_config=None,prev_step=None,process_name='cp',pipeline=None,**kwargs):
        if not prev_step is None:
            if pipeline_config is None:
                pipeline_config = MyConfigParser()
                pipeline_config.read(config.get('Pipeline',pipeline.obj_type))
            cp_input_dir_name = pipeline_config.safe_get('Common_directories','cp_subdir')
            if cp_input_dir_name is None:
                cp_input_dir_name = ""
                if prev_step.obj_type == "CleanBcbio":
                    cp_input_dir = prev_step.description
            cp_input_dir = os.path.join(pipeline.output_dir,cp_input_dir_name)
            output_subdir_name = pipeline_config.safe_get('Common_directories','output_subdir','ngv3')
            cp_dir = os.path.join(pipeline.input_dir,output_subdir_name)
            if not os.path.exists(cp_dir):
                os.makedirs(cp_dir)
            self.cp_dir = cp_dir
            SampleQsubProcess.__init__(self,config,key=key,input_dir=cp_input_dir,output_dir=pipeline.output_dir,process_name=process_name,**kwargs)
            if self.sample_key is not None:
                self.md5_file = os.path.join(cp_dir,self.sample_key + "_exome_md5checksums.txt")
            else:
                self.md5_file = "exome_md5checksums.txt"

    def __is_complete__(self,configs=None):
        return SampleQsubProcess.__is_complete__(self)
