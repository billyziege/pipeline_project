import os
import re
import sys
import shutil
from manage_storage.disk_queries import disk_usage
from physical_objects.hiseq.models import Sample
from processes.models import GenericProcess, QsubProcess
from template.scripts import fill_template
from sge_email.scripts import send_email
from processes.pipeline.bcbio_config_interaction import grab_yaml
from processes.hiseq.multi_fastq import list_from_multi_fastq_object

from sge_email.scripts import send_email

class DNANexusUpload(QsubProcess):
    """
	Upload CLIA Cancer samples feom ihg to Dna Nexus
    """

    def __init__(self,config,key=int(-1),input_dir=None,process_name='dnanexus_upload',pipeline_config=None,pipeline=None,**kwargs):
        """
	  Initializes the upload process object.
        """
        if not pipeline_config is None:
            output_name = os.path.basename(pipeline.input_dir)
            output_dir = os.path.join(pipeline_config.safe_get("Common_directories","dnanexus_storage"),output_name)
            QsubProcess.__init__(self,config,key=key,input_dir=pipeline.input_dir,output_dir=output_dir,process_name=process_name,**kwargs)
	    self.flowcell_key = pipeline.flowcell_key
            flowcell_dir = os.path.basename(pipeline.input_dir.rstrip('/'))
            self.run_qc_metrics_path = os.path.join(config.get('Common_directories','hiseq_run_log'),flowcell_dir + "/run_qc_metrics.txt")
            if not os.path.isfile(self.run_qc_metrics_path):
                #Send an email that run qc metrics file is missing.
                subject = "Missing run_qc_metrics for " + self.flowcell_key
                message = "The run qc metrics file in the following path is missing:\n\t" + self.run_qc_metrics_path
                message += "\nUploading to DNANexus failed." 
                recipients = pipeline_config.safe_get("Email","standard_recipients")
                send_email(subject,message,recipients)
            self.flowcell_dir_name = os.path.basename(self.input_dir)
            self.hiseq_run_log_dir = os.path.join(config.get("Common_directories","hiseq_run_log"),self.flowcell_dir_name) #Look at other object to how to get things from the sys config.
            self.upload_failed = False

    def __is_complete__(self,configs,mockdb,*args,**kwargs):
        if GenericProcess.__is_complete__(self,*args,**kwargs):
            return True
        if not os.path.isfile(self.stderr):
            return False
        if os.stat(self.stderr)[6] != 0 and self.upload_failed is False:
            subject = "DNANexus uploading error for " + self.flowcell_key
            message = "DNANexus uploading has encountered an error.  This error is detailed here:\n\t" + self.stderr
            message += "\nThe process has been halted, and the qsub script may be found here:\n\t" + self.qsub_file 
            recipients = configs["pipeline"].safe_get("Email","standard_recipients")
            send_email(subject,message,recipients)
            self.upload_failed = True
            return False
        return True


    def __finish__(self,*args,**kwargs):
       QsubProcess.__finish__(self,*args,**kwargs)
       shutil.rmtree(self.output_dir)


    #def __finish__(seconfigs,*args,):
    #    QsubProcess.__finish__()
        #Send an email that this directory is done. (send_email(args))
