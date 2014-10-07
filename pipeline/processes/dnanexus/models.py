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


class DNANexusUpload(QsubProcess):
    """
	Upload CLIA Cancer samples feom ihg to Dna Nexus
    """
def __init__(self,config,key=int(-1),pipeline=None,pipeline_config=None,process_name='dnanexusupload',**kwargs):
        """
	  Initializes the zcat process object.
        """
        if not self.pipeline_config is None:
            QsubProcess.__init__(self,config,key=key,input_dir=pipeline.input_dir,process_name=process_name,**kwargs)
	    self.flowcell_key = pipeline.flowcell_key
            flowcell_dir = basename(pipeline.input_dir)
            self.run_qc_metrics_dir = os.path.join(config.get('Common_directories','qc_metrics_run_log'),flowcell_dir)
            if not os.path.isfile(os.path.join(self.run_qc_metrics_dir),"run_qc_metrics.txt"):
                #Send an email that run qc metrics file is missing. (send_email(subject,message))
                raise Exception ("Run QC metrics file is missing")

    def __finish__(configs):
        QsubProcess.__finish__()
        #Send an email that this directory is done. (send_email(subject,message,recipients))
