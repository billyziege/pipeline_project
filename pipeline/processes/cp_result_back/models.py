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
            cp_input_dir_name = pipeline_config.safe_get('Common_directories','cp_subdir','results')
            cp_input_dir = os.path.join(pipeline.output_dir,cp_input_dir_name)
            output_subdir_name = pipeline_config.safe_get('Common_directories','output_subdir','ngv3')
            cp_dir = os.path.join(pipeline.input_dir,output_subdir)
            if not os.path.exists(cp_dir):
                os.makedirs(cp_dir)
            self.cp_dir = cp_dir
            SampleQsubProcess.__init__(self,config,key=key,input_dir=cp_input_dir,output_dir=pipeline.output_dir,process_name=process_name,**kwargs)
            if sample is not None:
                self.md5_file = os.path.join(cp_dir,sample.key + "_exome_md5checksums.txt")
            else:
                self.md5_file = "exome_md5checksums.txt"

    def __is_complete__(self,*args,**kwargs):
        return SampleQsubProcess.__is_complete__(self,*args,**kwargs)

def begin_cp_result_back(configs,mockdb,pipeline,step_objects,prev_step_key,**kwargs):
    sample = mockdb['Sample'].__get__(configs['system'],pipeline.sample_key)
    if configs['pipeline'].has_option('Common_directories','output_subdir'):
        output_subdir = configs['pipeline'].get('Common_directories','output_subdir')
    else:
        output_subdir = 'ngv3'
    if configs['pipeline'].has_option('Common_directories','cp_subdir'):
        if configs['pipeline'].get('Common_directories','cp_subdir') == "None":
            cp_input_dir = pipeline.output_dir
        else:
            cp_input_dir = os.path.join(pipeline.output_dir,configs['pipeline'].get('Common_directories','cp_subdir'))
    else:
        cp_input_dir = os.path.join(pipeline.output_dir,"results")
    cp_result_back = mockdb['CpResultBack'].__new__(configs['system'],sample=sample,input_dir=cp_input_dir,output_dir=pipeline.output_dir,base_output_dir=pipeline.input_dir,output_subdir=output_subdir,process_name='cp')
    cp_result_back.__fill_qsub_file__(configs)
    cp_result_back.__launch__(configs['system'])
    pipeline.cp_result_back_key = cp_result_back.key
    return cp_result_back


