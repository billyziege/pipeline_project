

class DNANexusUpload(SampleQsubProcess):
    """
    """

    def __init__(self,config,key=int(-1),process_name='fastqc',**kwargs):
        """
        """
        SampleQsubProcess.__init__(self,config,key=key,process_name=process_name,**kwargs)
