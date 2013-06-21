import os
import re
import sys
from manage_storage.disk_queries import disk_usage
from physical_objects.hiseq.models import Sample
from processes.models import SampleQsubProcess
from template.scripts import fill_template
from sge_email.scripts import send_email

class Backup(SampleQsubProcess):
    """
    Manage and stores info for the Zcat process.  This is the process that decompresses and moves fastq files from storage to the processing directories. 
    """

    def __init__(self,config,key=int(-1),sample=None,base_output_dir=None,location=None,process_name='backup',**kwargs):
        """
        Initializes the backup process object.  Requires config details
        """
        if base_output_dir is None:
            base_output_dir = config.get('Backup','output_dir')
        SampleQsubProcess.__init__(self,config,key=key,sample=sample,base_output_dir=base_output_dir,process_name=process_name,**kwargs)
        self.retry = 0
        if location is None:
            self.location = config.get('Backup','dir_name')

    def __fill_qsub_file__(self,config,r_list=None):
        """
        Fills the qsub file from a template.  Since not all information is archived in the parent object, 
        the function also gets additional information on the fly for the qsub file.
        """
        template_file= os.path.join(config.get('Common_directories','template'),'generic_qsub.template')
        if r_list is None:
            r_list = [ fname for fname in os.listdir(self.input_dir) if re.search("R[1,2]\w*\.fastq",fname) ]
        dictionary = {}
        for k,v in self.__dict__.iteritems():
            dictionary.update({k:str(v)})
        commands = ""
        copy = config.get('Backup','copy')
        keygen = config.get('Backup','generate_key')
        for fname in r_list:
            key_in = os.path.join(config.get('Backup','key_repository'),fname + config.get('Backup','key_extension'))
            key_out = os.path.join(self.output_dir,fname + config.get('Backup','key_extension'))
            commands += "cd " + self.input_dir + "\n" + copy   + " " + fname + " " + self.output_dir + "\n"
            commands += "cd " + self.input_dir + "\n" + keygen + " " + fname + " > " + key_in + "\n"
            commands += "cd " + self.output_dir + "\n" + keygen + " " + fname + " > " + key_out + "\n"
        dictionary.update({'commands': commands})
        with open(self.qsub_file,'w') as f:
            f.write(fill_template(template_file,dictionary))

    def __is_complete__(self,config,storage_device):
        """
        Check the complete file of the backup process, retry copying files
        where the keys for the input and output files are not the
        same, and handles notifications (if any).
        """
        if not os.path.isfile(self.complete_file):
            return False
        failed_files = self.__failed_files__(config)
        if len(failed_files) > 0:
            if self.retry >= config.get('Backup','retry_threshold'):
                send_email(self.__generate_repeated_error_text__(config,failed_files))
            self.__fill_qsub_file__(config,r_list=failed_files)
            self.__launch__(config,storage_device)
            self.retry += 1
            return False
        return True

    def __failed_files__(self,config,r_list=None):
        failed_files = []
        if r_list is None:
            r_list = [ fname for fname in os.listdir(self.input_dir) if re.search("R[1,2]\w*\.fastq",fname) ]
        for fname in r_list:
            key_in = os.path.join(config.get('Backup','key_repository'),fname + config.get('Backup','key_extension'))
            key_out = os.path.join(self.output_dir,fname + config.get('Backup','key_extension'))
            with open(key_in,'r') as f:
                in_string = f.read()
            with open(key_out,'r') as f:
                out_string = f.read()
            if not in_string == out_string:
                failed_files.append(fname)
        return failed_files

    def __generate_repeated_error_text__(self,config,failed_files):
        template_subject = os.path.join(config.get('Common_directories','template'),config.get('Backup_email_templates','repeated_subject'))
        template_body = os.path.join(config.get('Common_directories','template'),config.get('Backup_email_templates','repeated_body'))
        dictionary = {}
        for k,v in self.__dict__.iteritems():
            dictionary.update({k:str(v)})
        dictionary.update({'failed_files': "\n".join(failed_files)})
        subject = fill_template(template_subject,dictionary)
        body = fill_template(template_body,dictionary)
        return subject, body

    def __generate_storage_error_text__(self,config,storage_device):
        template_subject = os.path.join(config.get('Common_directories','template'),config.get('Backup_email_templates','storage_subject'))
        template_body = os.path.join(config.get('Common_directories','template'),config.get('Backup_email_templates','storage_body'))
        dictionary = {}
        for k,v in self.__dict__.iteritems():
            dictionary.update({k:str(v)})
        dictionary.update({'available': str(storage_device.available)})
        subject = fill_template(template_subject,dictionary)
        body = fill_template(template_body,dictionary)
        return subject, body

    def __generate_full_error_text__(self,config,storage_device):
        template_subject = os.path.join(config.get('Common_directories','template'),config.get('Backup_email_templates','full_subject'))
        template_body = os.path.join(config.get('Common_directories','template'),config.get('Backup_email_templates','full_body'))
        dictionary = {}
        for k,v in self.__dict__.iteritems():
            dictionary.update({k:str(v)})
        dictionary.update({'available': str(storage_device.available)})
        dictionary.update({'required_fastq_size': str(config.get('Storage','required_fastq_size'))})
        subject = fill_template(template_subject,dictionary)
        body = fill_template(template_body,dictionary)
        return subject, body

    def __launch__(self,config,storage_device,node_list=None):
        """
        Checks to make sure there is enough storage.  If
        not, sends email.  If so, sends the job to SGE and 
        records pertinent information.
        """
        #If storage device is full, send a notification and abort.
        if storage_device.__is_full__(config.get('Storage','required_fastq_size')):
            send_email(self.__generate_full_error_text__(config,storage_device))
            return False
        #This differs from the previous check by the fact that the previous does not
        #account for jobs that are currently being copied.  This error is not as 
        #restrictive due to the fact that the required_fastq_size should be larger than
        #the actual fastq size thus leaving additional storage once complete.
        if not storage_device.__is_available__(config.get('Storage','required_fastq_size')) and self.fail_reported == False:
            send_email(self.__generate_storage_error_text__(config,storage_device))
            self.fail_reported = True
            return False
        if node_list is None:
            node_list = config.get('Backup','nodes')
        SampleQsubProcess.__launch__(self,config,node_list=node_list,queue_name='single')
        return True

