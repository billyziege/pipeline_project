import os
import re
import sys
from physical_objects.models import Sample
from physical_objects.hiseq.models import Flowcell
from processes.models import GenericProcess, SampleQsubProcess
from processes.pipeline.extract_stats import store_stats_in_db
from processes.pipeline.bcbio_config_interaction import grab_yaml
from template.scripts import fill_template
from sge_email.scripts import send_email

class Bcbio(SampleQsubProcess):
    """
    Manages and stores information for the bcbio process.  This process runs the fastq to vcf conversion implemented by Brad Chapmann.
    """

    def __init__(self,config,key=int(-1),sample=None,flowcell=None,base_output_dir=None,r1_path=None,r2_path=None,description=None,upload_dir=None,process_name='bcbio',capture_target_bed=None,**kwargs):
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
        if os.path.isfile(self.sample_file):
            sample_yaml = grab_yaml(self.sample_file)
        else:
            sample_yaml = {}
        #snp_filename = self.sample_key + "_R_" + str(self.date_begin) + "_" + self.flowcell_key + "-sort-dup-gatkrecal-realign-variants-snp.vcf"
        try:
            bcbio_lane_name = sample_yaml["details"][0]["lane"]
        except KeyError: 
            bcbio_lane_name = None
        if not capture_target_bed is None:
            bait_bed = capture_target_bed
        else: 
            try:
                bait_bed = sample_yaml["details"][0]["algorithm"]["hybrid_bait"]
            except KeyError: 
                bait_bed = None
        #exit(str(bcbio_lane_name)+"\n")
        if bait_bed is None:
          if bcbio_lane_name is None:
              snp_filename = "gatk/1_" + str(self.date_begin) + "_" + self.flowcell_key + "-sort-dup-gatkrecal-realign-variants-snp.vcf"
          else:
              snp_filename = "gatk/"+bcbio_lane_name+"_" + str(self.date_begin) + "_" + self.flowcell_key + "-sort-dup-gatkrecal-realign-variants-snp.vcf"
          self.analysis_ready_bam_path = None
        else:
          snp_filename = "gatk/" + str(sample.key) + "_" + str(self.date_begin) + "_" + self.flowcell_key + "-sort-variants-ploidyfix-snp.vcf"
          combined_variant_filename = "gatk/" + str(sample.key) + "_" + str(self.date_begin) + "_" + self.flowcell_key + "-sort-variants-ploidyfix-combined.vcf"
          self.combined_variant_path = os.path.join(self.output_dir,combined_variant_filename)
          bam_filename = "bamprep/" + str(description) + "/" + str(sample.key) + "_" + str(self.date_begin) + "_" + self.flowcell_key + "-sort-prep.bam"
          self.analysis_ready_bam_path = os.path.join(self.output_dir, bam_filename) 
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
        self.gc_content = None
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
    
    def __fill_all_templates__(self,configs):
        """
        Multiple templates are used for the bcbio process.  This wraps filling all templates.
        """
        template_dir = configs['system'].get('Common_directories','template')
        sample_template = os.path.join(template_dir,configs['pipeline'].get('Template_files','sample'))
        system_template = os.path.join(template_dir,configs['pipeline'].get('Template_files','system'))
        qsub_template = os.path.join(template_dir,configs['pipeline'].get('Template_files','bcbio'))
        self.__fill_template__(sample_template,self.sample_file)
        self.__fill_template__(system_template,self.systems_file)
        self.__fill_template__(qsub_template,self.qsub_file)
        
    def __snps_called__(self):
        """
        This checks to see if the sub-process snp_stats has run.
        """
        directory, filename = os.path.split(self.snp_path)
        basename = os.path.basename(directory)
        if basename != 'gatk':
            new_directory = os.path.join(directory,'gatk')
            new_filename = re.sub(self.sample_key + "_R_","1_",filename)
            snp_path = os.path.join(new_directory,new_filename)
            self.snp_path = snp_path
        if not os.path.isfile(self.snp_path):
            alternative_path = os.path.join(self.upload_dir,self.description + "/" + self.description + "-gatk.vcf")
            self.snp_path = alternative_path
            if not os.path.isfile(alternative_path):
                return False
        return True

    def __is_complete__(self,configs,*args,**kwargs):
        """
        Due to the inclusion of sub-processes (snp_stats and concordance search),
        this function contains the logic to check to makes sure all of these processes
        have completed successfully.  If complete, the relevant statistics are stored.
        """
        current_dir = self.output_dir
        if GenericProcess.__is_complete__(self,*args,**kwargs):
            return True
        elif not os.path.isfile(self.complete_file):
            if hasattr(self,"upload_dir"):
                current_dir = self.upload_dir
                if not os.path.isfile(self.complete_file.replace(self.output_dir,self.upload_dir)): #If the output directory has already been cleaned, check the upload dir.
                    return False
            else: 
                return False
        if hasattr(self, "snp_path") and not self.snp_path is None and hasattr(self,"analysis_ready_bam_path") and not self.analysis_ready_bam_path is None:
            if not os.path.isdir(os.path.dirname(self.snp_path)) or not os.path.dirname(os.path.isfile(self.analysis_ready_bam_path)):
                return False
            if not os.path.isfile(self.snp_path) or not os.path.isfile(self.analysis_ready_bam_path):
                snp_file = False
                bam_file = False
                return False
                if not self.upload_dir is None:
                    for file in os.listdir(os.path.join(self.upload_dir,self.description)):
                        if file.endswith('.vcf'):
                            snp_file = True 
                        if file.endswith('.bam'):
                            bam_file = True 
                if not snp_file or not bam_file:
                    if configs["system"].get("Logging","debug") is "True":
                        print "At least one of the output files is missing for sample " + str(self.sample_key) + ":"
                        if not os.path.isfile(self.snp_path):
                            print "Missing "+ self.snp_path
                        if not os.path.isfile(self.analysis_ready_bam_path):
                            print "Missing "+ self.analysis_ready_bam_path
                #os.remove(self.complete_file)
                #template_dir = configs['system'].get('Common_directories','template')
                #qsub_template = os.path.join(template_dir,configs['pipeline'].get('Template_files','bcbio_no_postprocess'))
                #self.__fill_template__(qsub_template,os.path.join(self.output_dir,"bcbio_no_postprocess.sh"))
                #self.__launch__(configs['system'],os.path.join(self.output_dir,"bcbio_no_postprocess.sh"))
                    return False
        else:
            check_file = os.path.join(current_dir,'project-summary.csv')
        #If the process is complete, check to make sure that the check file is created.  If not, send email once.
            if not os.path.isfile(check_file) and configs['pipeline'].has_option('Template_files','bcbio_no_postprocess') and current_dir==self.output_dir:
            #subject, body = self.__generate_general_error_text__(config)
            #send_email(subject,body)
            #self.fail_reported = True
                os.remove(self.complete_file)
                template_dir = configs['system'].get('Common_directories','template')
                qsub_template = os.path.join(template_dir,configs['pipeline'].get('Template_files','bcbio_no_postprocess'))
                self.__fill_template__(qsub_template,os.path.join(self.output_dir,"bcbio_no_postprocess.sh"))
                self.__launch__(configs['system'],os.path.join(self.output_dir,"bcbio_no_postprocess.sh"))
                return False
        #store_stats_in_db(self)
        self.__finish__(*args,**kwargs)
        return True

    def __generate_general_error_text__(self,config):
        template_subject = os.path.join(config.get('Common_directories','template'),config.get('Bcbio_email_templates','general_subject'))
        template_body = os.path.join(config.get('Common_directories','template'),config.get('Bcbio_email_templates','general_body'))
        subject = fill_template(template_subject,self.__dict__)
        body = fill_template(template_body, self.__dict__)
        return subject, body

    def __launch__(self,config,command=None,**kwargs):
        """
        Sends the job to SGE and records pertinent information.
        """
        if command is None:
            command = ['sleep 30;','qsub']
        return SampleQsubProcess.__launch__(self,config,command=command,**kwargs)

