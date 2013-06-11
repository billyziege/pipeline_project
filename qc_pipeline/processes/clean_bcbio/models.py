import os
import re
from processes.hiseq.models import Sample
from processes.models import QsubProcess
from template.scripts import fill_template

class CleanBcbio(QsubProcess):

    def __init__(self,config,key=int(-1),sample=None,input_dir=None,base_output_dir=None,output_dir=None,process_name='clean',**kwargs):
        if sample is None:
            sample = Sample(config,key="dummy_sample_key")
        if sample.__class__.__name__ != "Sample":
            raise Exception("Trying to start a cleaning process on a non-sample.")
        QsubProcess.__init__(self,config,key=key,sample=sample,input_dir=input_dir,base_output_dir=base_output_dir,output_dir=output_dir,process_name=process_name,**kwargs)

    def __fill_qsub_file__(self,config):
        template_file= os.path.join(config.get('Common_directories','template'),config.get('Template_files','clean'))
        dictionary = {}
        with open(self.qsub_file,'w') as f:
            f.write(fill_template(template_file,self.__dict__))

    def __is_complete__(self,config):
        if os.path.isfile(self.complete_file):
            pass
        else:
            return False
        return True
