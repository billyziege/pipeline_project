import os
import re
import sys
from physical_objects.models import Sample
from physical_objects.hiseq.models import Flowcell
from processes.models import SampleQsubProcess
from processes.pipeline.extract_stats import grab_project_summary_stats, store_stats_in_db
from template.scripts import fill_template
from sge_email.scripts import send_email

class Bcbio(SampleQsubProcess):
    """
    Manages and stores information for the bcbio process.  This process runs the fastq to vcf conversion implemented by Brad Chapmann.
    """

    def __init__(self,config,key=int(-1),sample=None,flowcell=None,base_output_dir=None,r1_path=None,r2_path=None,description=None,upload_dir=None,process_name='bcbio',**kwargs):
        """
        Initializes the bcbio process object.
        """
        if flowcell is None:
            flowcell = Flowcell(config,key="dummy_flowcell_key")
        if flowcell.__class__.__name__ != "Flowcell":
            raise Exception("Trying to start a bcbio process on a non-flowcell.")
        SampleQsubProcess.__init__(self,config,key=key,sample=sample,base_output_dir=base_output_dir,process_name=process_name,**kwargs)
        self.input_dir = self.output_dir
        self.r1_path = r1_path
        self.r2_path = r2_path
        self.systems_file = os.path.join(self.input_dir,'system.yaml')
        self.sample_file = os.path.join(self.input_dir,'sample.yaml')
        self.upload_dir = upload_dir
        self.flowcell_key = flowcell.key
        self.description = description
        snp_filename = self.sample_key + "_R_" + str(self.date_begin) + "_" + self.flowcell_key + "-sort-dup-gatkrecal-realign-variants-snp.vcf"
        self.snp_path = os.path.join(self.output_dir,snp_filename)
        sort_dup_bam = self.sample_key + "_R_" + str(self.date_begin) + "_" + self.flowcell_key + "-sort-dup.bam"
        self.sort_dup_bam = os.path.join(self.output_dir,sort_dup_bam)
        self.project_summary_file = os.path.join(self.output_dir,config.get('Filenames','project_summary'))
        self.restats_file = os.path.join(self.output_dir,config.get('Filenames','restats'))
        #Stats for this process
        self.total_reads = None
        self.percent_aligned = None
        self.percentage_duplicates = None
        self.insert_size = None
        self.percentage_on_target_bases = None
        self.mean_target_coverage = None
        self.percentage_with_at_least_10x_coverage = None
        self.percentage_0x_coverage = None
        self.total_variations = None
        self.percentage_in_db_snp = None
        self.titv_all = None
        self.titv_dbsnp = None
        self.titv_novel = None

    def __fill_template__(self,template_file,output_fname):
        """
        Since the bcbio object does not retain all the information necessary for 
        some of the templates, this finds and adds the additional information and
        then fills the template file and writes as the output file.
        """
        dictionary = {}
        for k,v in self.__dict__.iteritems():
            if k == 'sample_key':
                try:
                    int(v)
                    new_sample_key = "Sample_" + str(v)
                    dictionary.update({k:new_sample_key})
                    continue
                except ValueError:
                    pass
            dictionary.update({k:str(v)})
        dictionary.update({'restats_tail': self.restats_file + '.tail'})
        with open(output_fname,'w') as f:
            string = fill_template(template_file,dictionary)
            f.write(string)
    
    def __fill_all_templates__(self,config):
        """
        Multiple templates are used for the bcbio process.  This wraps filling all templates.
        """
        template_dir = config.get('Common_directories','template')
        sample_template = os.path.join(template_dir,config.get('Template_files','sample'))
        system_template = os.path.join(template_dir,config.get('Template_files','system'))
        qsub_template = os.path.join(template_dir,config.get('Template_files','bcbio'))
        self.__fill_template__(sample_template,self.sample_file)
        self.__fill_template__(system_template,self.systems_file)
        self.__fill_template__(qsub_template,self.qsub_file)
        
    def __snps_called__(self):
        """
        This checks to see if the sub-process snp_stats has run.
        """
        return os.path.isfile(self.snp_path)

    def __is_complete__(self,config):
        """
        Due to the inclusion of sub-processes (snp_stats and concordance search),
        this function contains the logic to check to makes sure all of these processes
        have completed successfully.  If complete, the relevant statistics are stored.
        """
        if GenericProcess.__is_complete__():
            return True
        elif not os.path.isfile(self.complete_file):
            return False
        check_file = os.path.join(self.output_dir,'project-summary.csv')
        #If the process is complete, check to make sure that the check file is created.  If not, send email once.
        if not os.path.isfile(check_file) and self.fail_reported == False:
            send_email(self.__generate_general_error_text__(config))
            self.fail_reported = True
            return False
        store_stats_in_db(self)
        return True

    def __generate_general_error_text__(self,config):
        template_subject = os.path.join(config.get('Common_directories','template'),config.get('Bcbio_email_templates','general_subject'))
        template_body = os.path.join(config.get('Common_directories','template'),config.get('Bcbio_email_templates','general_body'))
        subject = fill_template(template_subject,self.__dict__)
        body = fill_template(template_body, self.__dict__)
        return subject, body
