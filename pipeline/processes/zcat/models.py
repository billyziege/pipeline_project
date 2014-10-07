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

class GenericCp(QsubProcess):
    """
    A class for processes that don't necessarily need sample data.
    """
    def __init__(self,config,key=-1,pipeline_config=None,base_output_dir=None,output_sub_dir=None,input_dir=None,process_name='generic_cp',**kwargs):
        if not pipeline_config is None:
            output_dir = None
            if not base_output_dir is None and not output_sub_dir is None:
                output_dir = os.path(base_output_dir,output_sub_dir)
            QsubProcess(self,config,key=key,output_dir=output_dir,input_dir=input_dir,process_name=process_name,**kwargs)
            
            

class Zcat(SampleQsubProcess):
    """
    Manage and stores info for the Zcat process.  This is the process that decompresses and moves fastq files from storage to the processing directories. 
    """

    def __init__(self,config,process_name='zcat',**kwargs):
        """
        Initializes the zcat process object.
        """
        SampleQsubProcess.__init__(self,config,process_name=process_name,**kwargs)
        extension = ''
        r1_fname = self.sample_key + '_R1.fastq' + extension
        r2_fname = self.sample_key + '_R2.fastq' + extension
        self.r1_path = os.path.join(self.output_dir,r1_fname)
        self.r2_path = os.path.join(self.output_dir,r2_fname)

    def __fill_qsub_file__(self,configs):
        """
        Fills the qsub file from a template.  Since not all information is archived in the parent object, 
        the function also gets additional information on the fly for the qsub file.
        """
        template_file= os.path.join(configs['system'].get('Common_directories','template'),configs['pipeline'].get('Template_files',self.process_name))
        r1_list = [ os.path.join(self.input_dir,f) for f in os.listdir(self.input_dir) if re.search("R1[\.,\w]*\.fastq",f) ]
        r1_list.sort()
        list_of_r1_files = " ".join(r1_list)
        r2_list = [ os.path.join(self.input_dir,f) for f in os.listdir(self.input_dir) if re.search("R2[\.,\w]*\.fastq",f) ]
        r2_list.sort()
        list_of_r2_files = " ".join(r2_list)
        dictionary = {}
        for k,v in self.__dict__.iteritems():
            dictionary.update({k:str(v)})
        dictionary.update({'list_of_r1_files':list_of_r1_files})
        dictionary.update({'list_of_r2_files':list_of_r2_files})
        with open(self.qsub_file,'w') as f:
            f.write(fill_template(template_file,dictionary))

    def __is_complete__(self):
        """
        Check to the complete file of the zcat process and handles notifications (if any).
        """
        if GenericProcess.__is_complete__(self):
            return True
        elif not os.path.isfile(self.complete_file):
            #print self.complete_file
            return False
        #If the process is complete, check to make sure that the sizes of the file are adequate.  If not, send email.
        size1 = int(disk_usage(self.r1_path))
        size2 = int(disk_usage(self.r2_path))
        size = size2
        if size1 < size2:
            size = size1
        #Send an email if the size of the fastq is smaller than the expected size.
        #if size < int(configs['pipeline'].get('Storage','expected_fastq_size')):
            #template_subject = os.path.join(configs['system'].get('Common_directories','template'),configs['pipeline'].get('Zcat_email_templates','size_subject'))
            #template_body = os.path.join(configs['system'].get('Common_directories','template'),configs['pipeline'].get('Zcat_email_templates','size_body'))
            #dictionary = {}
            #for k,v in self.__dict__.iteritems():
            #    dictionary.update({k:str(v)})
            #dictionary.update({'size':size})
            #subject = fill_template(template_subject,dictionary)
            #body = fill_template(template_body, dictionary)
            #send_email(subject,body)
        return True

    def __launch__(self,config,node_list=None):
        """
        Checks to make sure there is enough storage.  If
        not, sends email.  If so, sends the job to SGE and 
        records pertinent information.
        """
        if node_list is None:
            node_list = config.get('Zcat','nodes')
        SampleQsubProcess.__launch__(self,config)
        #SampleQsubProcess.__launch__(self,config,node_list=node_list,queue_name='single')
        return True

class Cat(Zcat):
    def __init__(self,config,process_name='cat',**kwargs):
        Zcat.__init__(self,config,process_name=process_name,**kwargs)
        self.r1_path = self.r1_path + '.gz'
        self.r2_path = self.r2_path + '.gz'

class ZcatMultiple(SampleQsubProcess):
    """
    The regular zcat zcats all read files to two fastq files.  This method mvs and uncompresses
    each file individually for parallel analysis utilizing the Task management aspect of
    the QsubProcess.
    """

    def __init__(self,config,key=int(-1),process_name='zcat_multiple',multi_fastq_file=None,**kwargs):
        """
        Initializes the zcat multiple process object.
        """
        SampleQsubProcess.__init__(self,config,key=key,process_name=process_name,**kwargs)
        #Grab the first read files.
        r1_in_list = []
        if not multi_fastq_file is None:
            self.multi_fastq_file = multi_fastq_file
            multi_fastq = grab_yaml(self.multi_fastq_file)
            r1_in_list = list_from_multi_fastq_object(multi_fastq,"r1_filename")
        self.r1_input = ":".join(r1_in_list)
        r1_out_list = []
        r1_uncompressed_list = []
        for i in range(len(r1_in_list)):
            filename = self.sample_key + "_" + str(i) + "_R1.fastq" 
            r1_uncompressed_list.append(os.path.join(self.output_dir,filename))
            if r1_in_list[i][-3:] == '.gz':
                filename += ".gz"
            r1_out_list.append(os.path.join(self.output_dir,filename)) ##For the copy process
        self.r1_copied = ":".join(r1_out_list)
        self.r1_uncompressed = ":".join(r1_uncompressed_list)

        #Grab the paired read files.
        r2_in_list = []
        if not multi_fastq_file is None:
            r2_in_list = list_from_multi_fastq_object(multi_fastq,"r2_filename")
        self.r2_input = ":".join(r2_in_list)
        r2_out_list = []
        r2_uncompressed_list = []
        for i in range(len(r2_in_list)):
            filename = self.sample_key + "_" + str(i) + "_R2.fastq" 
            r2_uncompressed_list.append(os.path.join(self.output_dir,filename))
            if r2_in_list[i][-3:] == '.gz':
                filename += ".gz"
            r2_out_list.append(os.path.join(self.output_dir,filename))
        self.r2_copied = ":".join(r2_out_list)
        self.r2_uncompressed = ":".join(r2_uncompressed_list)

        if len(r1_in_list) == len(r2_in_list):
            self.number_tasks = len(r1_in_list)
            tmp_dirs = []
            complete_files = []
            for i in range(len(r1_in_list)):
                task_number = i + 1
                complete_file = os.path.join(self.output_dir, self.process_name + '.' + str(task_number) + '.complete')
                complete_files.append(complete_file)
                tmp_dir = os.path.join(self.output_dir, 'tmp.' + str(task_number))
                if not os.path.isdir(tmp_dir) and not re.search('dummy',self.output_dir):
                    os.makedirs(tmp_dir)
                tmp_dirs.append(tmp_dir)
            self.tmp_dir = ":".join(tmp_dirs)
            self.complete_file = ":".join(complete_files)
        else:
            raise Exception("The number of read and matched-pair read files for sample " + sample.key + " are not the same")
   

