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

from sge_email.scripts import send_email

class DNANexusUpload(QsubProcess):
    """
	Upload CLIA Cancer samples feom ihg to Dna Nexus
    """

    def __init__(self,config,key=int(-1),input_dir=None,process_name='dnanexus_upload',**kwargs):
        """
	  Initializes the upload process object.
        """
        if not self.pipeline_config is None:
            QsubProcess.__init__(self,config,key=key,input_dir=pipeline.input_dir,process_name=process_name,**kwargs)
	    self.flowcell_key = pipeline.flowcell_key
            flowcell_dir = basename(pipeline.input_dir)
            self.run_qc_metrics_dir = os.path.join(config.get('Common_directories','qc_metrics_run_log'),flowcell_dir)
            if not os.path.isfile(os.path.join(self.run_qc_metrics_dir),"run_qc_metrics.txt"):
                #Send an email that run qc metrics file is missing. (send_email(subject,message))
                raise Exception ("Run QC metrics file is missing")
            self.flowcell_dir_name = basename(self.input_dir)
            self.hiseq_run_log_dir = os.path.join(config.get("Common directories","hiseq_run_log"),self.flowcell_dir_name) #Look at other object to how to get things from the sys config.

    def __finish__(configs):
        SampleQsubProcess.__finish__()
        #Send an email that this directory is done. (send_email(args))
