import os
import re
from physical_objects.hiseq.models import Sample
from processes.models import SampleQsubProcess
from template.scripts import fill_template

class CleanBcbio(SampleQsubProcess):
    """
    Runs the clean bcbio process that removes superfluous information in the
    run directory and moves the result back to storage. 
    """

    def __fill_qsub_file__(self,config):
    """
    Simply fills the clean bcbio template with appropriate info. 
    """
        template_file= os.path.join(config.get('Common_directories','template'),config.get('Template_files','clean'))
        dictionary = {}
        with open(self.qsub_file,'w') as f:
            f.write(fill_template(template_file,self.__dict__))

    def __is_complete__(self,config):
    """
    Checks to see if the clean bcbio complete file has been created.
    """
        if os.path.isfile(self.complete_file):
            pass
        else:
            return False
        return True
