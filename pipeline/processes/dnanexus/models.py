from sge_email.scripts import send_email

class DNANexusUpload(SampleQsubProcess):
    """
    """

    def __init__(self,config,key=int(-1),input_dir=None,process_name='dnanexus_upload',**kwargs):
        """
        """
        if not self.input_dir is None:
            SampleQsubProcess.__init__(self,config,key=key,process_name=process_name,**kwargs)
            self.flowcell_dir_name = basename(self.input_dir)
            self.hiseq_run_log_dir = os.path.join(config.get("Common directories","hiseq_run_log"),self.flowcell_dir_name) #Look at other object to how to get things from the sys config.

    def __finish__(configs):
        SampleQsubProcess.__finish__()
        #Send an email that this directory is done. (send_email(args))
