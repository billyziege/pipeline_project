import os
import re
from manage_storage.disk_queries import disk_usage
from processes.hiseq.models import Sample
from processes.models import QsubProcess
from template.scripts import fill_template
from sge_email.scripts import send_email

class Zcat(QsubProcess):

    def __init__(self,config,key=int(-1),sample=None,input_dir=None,base_output_dir=None,process_name='zcat',**kwargs):
        if sample is None:
            sample = Sample(config,key="dummy_sample_key")
        if sample.__class__.__name__ != "Sample":
            raise Exception("Trying to start a zcat process on a non-sample.")
        QsubProcess.__init__(self,config,key=key,sample=sample,input_dir=input_dir,base_output_dir=base_output_dir,process_name=process_name,**kwargs)
        r1_fname = sample.key + '_R1.fastq'
        r2_fname = sample.key + '_R2.fastq'
        self.r1_path = os.path.join(self.output_dir,r1_fname)
        self.r2_path = os.path.join(self.output_dir,r2_fname)

    def __fill_qsub_file__(self,config):
        template_file= os.path.join(config.get('Common_directories','template'),'zcat.template')
        r1_list = [ os.path.join(self.input_dir,f) for f in os.listdir(self.input_dir) if re.search("R1\w*\.fastq",f) ]
        list_of_r1_files = " ".join(r1_list)
        r2_list = [ os.path.join(self.input_dir,f) for f in os.listdir(self.input_dir) if re.search("R2\w*\.fastq",f) ]
        list_of_r2_files = " ".join(r2_list)
        dictionary = {}
        for k,v in self.__dict__.iteritems():
            dictionary.update({k:str(v)})
        dictionary.update({'list_of_r1_files':list_of_r1_files})
        dictionary.update({'list_of_r2_files':list_of_r2_files})
        with open(self.qsub_file,'w') as f:
            f.write(fill_template(template_file,dictionary))

    def __is_complete__(self,config):
        if os.path.isfile(self.complete_file):
            pass
        else:
            return False
        #If the process is complete, check to make sure that the sizes of the file are adequate.  If not, send email.
        size1 = disk_usage(self.r1_path)
        size2 = disk_usage(self.r2_path)
        size = size2
        if size1 < size2:
            size = size1
        if size1 < config.get('Storage','expected_fastq_size') or size2 < config.get('Storage','expected_fastq_size'):
            template_subject = os.path.join(config.get('Common_directories','template'),config.get('Zcat_email_templates','size_subject'))
            template_body = os.path.join(config.get('Common_directories','template'),config.get('Zcat_email_templates','size_body'))
            dictionary = {}
            for k,v in self.__dict__.iteritems():
                dictionary.update({k:str(v)})
            dictionary.update({'size':size})
            subject = fill_template(template_subject,dictionary)
            body = fill_template(template_body, dictionary)
            send_email(subject,body)
        return True
