import os
import re
import sys
from manage_storage.disk_queries import disk_usage
from physical_objects.hiseq.models import Sample
from processes.models import GenericProcess, SampleQsubProcess
from template.scripts import fill_template
from sge_email.scripts import send_email
from processes.pipeline.bcbio_config_interaction import grab_yaml
from processes.hiseq.multi_fastq import list_from_multi_fastq_object

class FastQC(SampleQsubProcess):
    """
    Manage and stores info for the Zcat process.  This is the process that decompresses and moves fastq files from storage to the processing directories. 
    """

    def __init__(self,config,key=int(-1),prev_step=None,process_name='fastqc',input_dir=None,output_dir=None,**kwargs):
        """
        Initializes the zcat process object.
        """
        if not prev_step is None:
            SampleQsubProcess.__init__(self,config,key=key,input_dir=prev_step.output_dir,output_dir=prev_step.output_dir,process_name=process_name,**kwargs)
        elif not input_dir is None and not output_dir is None:
            SampleQsubProcess.__init__(self,config,key=key,input_dir=input_dir,output_dir=output_dir,process_name=process_name,**kwargs)

    def __is_complete__(self,*args,**kwargs):
        """
        Check to the complete file of the zcat process and handles notifications (if any).
        """
        try:
            if GenericProcess.__is_complete__(self):
                return True
            elif not os.path.isfile(self.complete_file):
                #print self.complete_file
                return False
            for filename in os.listdir(self.output_dir):
                if os.path.isfile(os.path.join(self.output_dir,filename)):
                    if (filename.endswith('.zip')):
                        if os.path.getsize(os.path.join(self.output_dir,filename)) > 0:
                            return True
            return False
        except:
            #sys.stderr.write("Error with fastq = " + str(self))
            return False
