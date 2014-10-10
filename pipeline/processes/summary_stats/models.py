import os
import sys
import re
from manage_storage.disk_queries import disk_usage
from physical_objects.models import Sample
from processes.pipeline.models import Bcbio
from processes.models import GenericProcess, SampleQsubProcess
from processes.summary_stats.extract_stats import store_summary_stats_in_db, store_snp_stats_in_db, grab_search_stats
from processes.pipeline.bcbio_config_interaction import get_genome_ref
from template.scripts import fill_template

class SummaryStats(SampleQsubProcess):
    """
    This manages and stores the information for the sub-process summary_stats.  Summarry_stats
    is run once bcbio is complete.  It generates the summary_statistics that are kept for run.
    """

    def __init__(self,config,key=int(-1),sample=None,bcbio=None,capture_target_bed=None,process_name='summary_stats',**kwargs):
        """
        Initializes the summary stats process.
        """
        if bcbio is None:
            bcbio = Bcbio(config,key=int(-1))
        if bcbio.__class__.__name__ != "Bcbio":
            raise Exception("Trying to start a summary_stats process on a non-bcbio pipeline.")
        self.capture_target_bed = capture_target_bed
        input_dir = bcbio.output_dir
        output_dir = os.path.join(bcbio.output_dir, "qc/"+str(bcbio.description))
        SampleQsubProcess.__init__(self,config,key=key,sample=sample,input_dir=input_dir,output_dir=output_dir,process_name=process_name,**kwargs)
        self.snp_path = bcbio.snp_path
        if not os.path.isfile(self.snp_path) and bcbio.key != -1:
            snp_dir = os.path.dirname(self.snp_path)
            for file in os.listdir(snp_dir):
                if file.endswith("combined-effects.vcf"):
                    self.snp_path = os.path.join(snp_dir,file)
        self.bam_path = bcbio.analysis_ready_bam_path
        if self.bam_path is None and not bcbio.description is None:
            bam_path = os.path.join(bcbio.output_dir,"bamprep/"+bcbio.description)
            for file in os.listdir(bam_path):
                if file.endswith("-sort-prep.bam"):
                    self.bam_path = os.path.join(bam_path,file)
                    break
            if self.bam_path is None:
                raise Exception("The previous process didn't finish correctly.")
        self.systems_file = bcbio.systems_file
        self.ref_path = get_genome_ref(bcbio.sample_file,bcbio.systems_file)
        if self.ref_path is None:
            self.ref_path = "Not_found"
        if config.has_section("Summary stats") and config.has_option("Summary stats","hethom_ext"):
            hethom_filename = self.sample_key + config.get("Summary stats","hethom_ext")
        else:
            hethom_filename = self.sample_key + ".hethom"
        self.hethom_path = os.path.join(self.output_dir,hethom_filename)
        if config.has_section("Summary stats") and config.has_option("Summary stats","indbsnp_ext"):
            indbsnp_filename = self.sample_key + config.get("Summary stats","indbsnp_ext")
        else:
            indbsnp_filename = self.sample_key + ".indbsnp"
        self.indbsnp_path = os.path.join(self.output_dir,indbsnp_filename)
        if config.has_section("Summary stats") and config.has_option("Summary stats","hs_metrics_ext"):
            hs_metrics_filename = self.sample_key + config.get("Summary stats","hs_metrics_ext")
        else:
            hs_metrics_filename = self.sample_key + ".hs_metrics"
        self.hs_metrics_path = os.path.join(self.output_dir,hs_metrics_filename)
        if config.has_section("Summary stats") and config.has_option("Summary stats","bamtools_stats_ext"):
            bamtools_stats_filename = self.sample_key + config.get("Summary stats","bamtools_stats_ext")
        else:
            bamtools_stats_filename = self.sample_key + ".bamtools_stats"
        self.bamtools_stats_path = os.path.join(self.output_dir,bamtools_stats_filename)
        if config.has_section("Summary stats") and config.has_option("Summary stats","vcfstats_stats_ext"):
            vcfstats_stats_filename = self.sample_key + config.get("Summary stats","vcfstats_stats_ext")
        else:
            vcfstats_stats_filename = self.sample_key + ".vcfstats_stats"
        self.vcfstats_stats_path = os.path.join(self.output_dir,vcfstats_stats_filename)
        self.summary_stats_path = os.path.join(self.output_dir,"summary-stats.csv")
        #Stats for this process
        self.hom = None
        self.het = None
        self.variants_total = None
        self.hethom_ratio = None
        self.total_reads = None
        self.percent_aligned = None
        self.percentage_duplicates = None
        self.insert_size = None
        self.percentage_on_target_bases = None
        self.percentage_near_target_bases = None
        self.mean_target_coverage = None
        self.percentage_with_at_least_10x_coverage = None
        self.percentage_0x_coverage = None
        self.percentage_in_db_snp = None
        self.ts_tv_ratio = None

    def __is_complete__(self,configs):
        """
        Since concordance search is an optional sub-process, this function checks for
        both the completeness of the self concordance program and the necessity,
        and if necessary the completeness, of the concordance search.  Then, once 
        complete, relevant statistics are stored.
        """
        if GenericProcess.__is_complete__(self):
            return True
        elif not os.path.isfile(self.complete_file):
            return False
        if os.stat(self.summary_stats_path)[6]==0:
            os.remove(self.complete_file)
            self.__launch__(configs['system'])
            return False
        if configs["system"].get("Logging","debug") is "True":
            print "  Storing stats" 
        store_summary_stats_in_db(self)
        if configs["system"].get("Logging","debug") is "True":
            print "  Finishing." 
        self.__finish__(*args,**kwargs)
        return True

class SnpStats(SampleQsubProcess):
    """
    This manages and stores the information for the sub-process snp_stats.  Snp_stats
    is a sub-process of bcbio.  It run concordance and hethom ratio generating scripts.
    """

    def __init__(self,config,key=int(-1),sample=None,bcbio=None,concordance_filename=None,hethom_filename=None,indbsnp_filename=None,process_name='snp_stats',**kwargs):
        """
        Initializes the snp stats process.
        """
        if bcbio is None:
            bcbio = Bcbio(config,key=int(-1))
        if bcbio.__class__.__name__ != "Bcbio":
            raise Exception("Trying to start a snp_stats process on a non-bcbio pipeline.")
        input_dir = bcbio.output_dir
        output_dir = input_dir
        SampleQsubProcess.__init__(self,config,key=key,sample=sample,input_dir=input_dir,output_dir=output_dir,process_name=process_name,**kwargs)
        self.snp_path = bcbio.snp_path
        if concordance_filename is None:
            concordance_filename = self.sample_key + ".con"
        self.concordance_path = os.path.join(self.output_dir,concordance_filename)
        if hethom_filename is None:
            hethom_filename = self.sample_key + ".hethom"
        self.hethom_path = os.path.join(self.output_dir,hethom_filename)
        if indbsnp_filename is None:
            indbsnp_filename = self.sample_key + ".indbsnp"
        self.indbsnp_path = os.path.join(self.output_dir,indbsnp_filename)
        #Stats for this process
        self.concordance_calls = None
        self.percentage_concordance = None
        self.hom = None
        self.het = None
        self.variants_total = None
        self.hethom_ratio = None
        self.in_dbsnp = None
        self.search_key = None

    def __fill_qsub_file__(self,configs):
        """
        Since some of the information for the qsub file is pulled from the config file,
        this finds that information and writes out the qsub file. 
        """
        template_file= os.path.join(configs['system'].get('Common_directories','template'),configs['pipeline'].get('Template_files','snp_stats'))
        dictionary = {}
        for k,v in self.__dict__.iteritems():
            dictionary.update({k:str(v)})
        dictionary.update({'vcf_conversion_script':configs['pipeline'].get('Concordance','vcf_conversion_script')})
        dictionary.update({'filter_file':configs['pipeline'].get('Filenames','snp_filter_file')})
        with open(self.qsub_file,'w') as f:
            f.write(fill_template(template_file,dictionary))

    def __is_complete__(self,configs,mockdb,*args,**kwargs):
        """
        Since concordance search is an optional sub-process, this function checks for
        both the completeness of the self concordance program and the necessity,
        and if necessary the completeness, of the concordance search.  Then, once 
        complete, relevant statistics are stored.
        """
        if GenericProcess.__is_complete__(self,*args,**kwargs):
            return True
        elif not os.path.isfile(self.complete_file):
            return False
        store_snp_stats_in_db(self)
        if self.percentage_concordance > configs['pipeline'].get('Concordance','threshold'):
            self.__finish__(*args,**kwargs)
            return True
        #If the concordance is below the threshold, we need to conduct a concordance search against the database
        #First we split the search across processors
        if self.search_key is None:
            sample = mockdb['Sample'].objects[self.sample_key]
            concord_search = mockdb['ConcordanceSearch'].__new__(configs['system'],sample=sample,snp_stats=self)
            self.search_key = concord_search.key
            concord_search.__launch_split_searches__(configs)
            return False
        concord_search = mockdb['ConcordanceSearch'].objects[self.search_key]
        if concord_search.__is_complete__(configs['system'],*args,**kwargs):
            self.__finish__(*args,**kwargs)
            return True
        #Now we gather
        if concord_search.__are_split_searches_complete__(configs['pipeline']):
            if os.path.isfile(concord_search.qsub_file):
                return False
            concord_search.__fill_qsub_file__(configs)
            concord_search.__launch__(configs['system'])
            return False
        return False

class ConcordanceSearch(SampleQsubProcess):
    """
    Handles the optional step of search the data base
    by calculating concordance of every sample and the 
    target sample.
    """

    def __init__(self,config,key=int(-1),sample=None,snp_stats=None,output_filename=None,process_name='concord_search',**kwargs):
        """
        Initializes the concordance search process.
        """
        if snp_stats is None:
            snp_stats = SnpStats(config,key=int(-1))
        if snp_stats.__class__.__name__ != "SnpStats":
            raise Exception("Trying to start a concordance search process for a non-snp_stats process.")
        input_dir = snp_stats.output_dir
        output_dir = input_dir
        SampleQsubProcess.__init__(self,config,key=key,sample=sample,input_dir=input_dir,output_dir=output_dir,process_name=process_name,**kwargs)
        self.snp_path = snp_stats.snp_path
        self.first_match = self.sample_key
        self.first_concordance = snp_stats.percentage_concordance
        self.second_match = None
        self.second_concordance = None
        self.third_match = None
        self.third_concordance = None
        self.fourth_match = None
        self.fourth_concordance = None
        self.fifth_match = None
        self.fifth_concordance = None
        self.sub_qsub_file_front = os.path.join(self.output_dir,"against_")
        if output_filename is None:
            output_filename = self.sample_key + "_all.con"
        self.output_path = os.path.join(self.output_dir,output_filename)

    def __fill_sub_qsub_files__(self,configs):
        """
        This handles scattering the search across nodes.
        """
        template_file= os.path.join(configs['system'].get('Common_directories','template'),configs['pipeline'].get('Template_files','individual_search'))
        list_files = configs['pipeline'].get('Concordance','split_lists').split(',')
        dictionary = {}
        for k,v in self.__dict__.iteritems():
            dictionary.update({k:str(v)})
        for list_file in list_files:
            name = re.sub('.ls','',os.path.basename(list_file))
            qsub_file = self.sub_qsub_file_front + name + '.sh'
            output_file = self.sub_qsub_file_front + name + '.con'
            complete_file = self.sub_qsub_file_front + name + '.complete'
            dictionary.update({'search_list':list_file})
            dictionary.update({'output_file':output_file}) #Overwrites the object's output file (not saved)
            dictionary.update({'complete_file':complete_file}) #Overwrites the object's complete file (not saved)
            dictionary.update({'vcf_conversion_script':configs['pipeline'].get('Concordance','vcf_conversion_script')})
            dictionary.update({'filter_file':configs['pipeline'].get('Filenames','snp_filter_file')})
            with open(qsub_file,'w') as f:
                f.write(fill_template(template_file,dictionary))

    def __fill_qsub_file__(self,configs):
        """
        This handles the qsub file which gathers the results of the scattered
        processes.
        """
        template_file= os.path.join(configs['system'].get('Common_directories','template'),configs['pipeline'].get('Template_files','concord_search'))
        list_files = configs['pipeline'].get('Concordance','split_lists').split(',')
        dictionary = {}
        for k,v in self.__dict__.iteritems():
            dictionary.update({k:str(v)})
        individual_search_outputs = []
        for list_file in list_files:
            name = re.sub('.ls','',os.path.basename(list_file))
            output_file = self.sub_qsub_file_front + name + '.con'
            individual_search_outputs.append(output_file)
        dictionary.update({'split_files':" ".join(individual_search_outputs)})
        with open(self.qsub_file,'w') as f:
            f.write(fill_template(template_file,dictionary))

    def __launch_split_searches__(self,configs):
        """
        This launches the scattering processes.
        """
        self.__fill_sub_qsub_files__(configs)
        list_files = configs['pipeline'].get('Concordance','split_lists').split(',')
        for list_file in list_files:
            name = re.sub('.ls','',os.path.basename(list_file))
            qsub_file = self.sub_qsub_file_front + name + '.sh'
            self.__launch__(configs['system'],qsub_file=qsub_file)

    def  __are_split_searches_complete__(self,config):
        """
        This checks to see if all of the scattered processes are complete ---
        if so, return True.
        """
        list_files = config.get('Concordance','split_lists').split(',')
        for list_file in list_files:
            name = re.sub('.ls','',os.path.basename(list_file))
            output_file = self.sub_qsub_file_front + name + '.con'
            complete_file = self.sub_qsub_file_front + name + '.complete'
            if not os.path.isfile(complete_file):
                return False
            if not os.path.isfile(output_file):
                raise Exception("The process for {0} is complete but the output is missing".format(output_file))
        return True

    def __is_complete__(self,config,*args,**kwargs):
        """
        Checks to see if the gathering process is complete.
        If so, the top 5 "scoring" results of the search are
        stored.
        """
        if GenericProcess.__is_complete__(self,*args,**kwargs):
            return True
        elif not os.path.isfile(self.complete_file):
            return False
        return True
