import os
import re
from manage_storage.disk_queries import disk_usage
from processes.hiseq.models import Sample
from processes.pipeline.models import Bcbio
from processes.models import QsubProcess
from processes.snp_stats.extract_stats import store_snp_stats_in_db, grab_search_stats
from template.scripts import fill_template

class SnpStats(QsubProcess):

    def __init__(self,config,key=int(-1),sample=None,bcbio=None,concordance_filename=None,hethom_filename=None,process_name='snp_stats',**kwargs):
        if sample is None:
            sample = Sample(config,key="dummy_sample_key")
        if bcbio is None:
            bcbio = Bcbio(config,key=int(-1))
        if bcbio.__class__.__name__ != "Bcbio":
            raise Exception("Trying to start a snp_stats process on a non-bcbio pipeline.")
        input_dir = bcbio.output_dir
        output_dir = input_dir
        QsubProcess.__init__(self,config,key=key,sample=sample,input_dir=input_dir,output_dir=output_dir,process_name=process_name,**kwargs)
        self.snp_path = bcbio.snp_path
        if concordance_filename is None:
            concordance_filename = self.sample_key + ".con"
        self.concordance_path = os.path.join(self.output_dir,concordance_filename)
        if hethom_filename is None:
            hethom_filename = self.sample_key + ".hethom"
        self.hethom_path = os.path.join(self.output_dir,hethom_filename)
        #Stats for this process
        self.concordance_calls = None
        self.percentage_concordance = None
        self.hom = None
        self.het = None
        self.variants_total = None
        self.hethom_ratio = None
        self.search_key = None

    def __fill_qsub_file__(self,config):
        template_file= os.path.join(config.get('Common_directories','template'),config.get('Template_files','snp_stats'))
        dictionary = {}
        for k,v in self.__dict__.iteritems():
            dictionary.update({k:str(v)})
        dictionary.update({'vcf_conversion_script':config.get('Concordance','vcf_conversion_script')})
        dictionary.update({'filter_file':config.get('Filenames','snp_filter_file')})
        with open(self.qsub_file,'w') as f:
            f.write(fill_template(template_file,dictionary))

    def __is_complete__(self,config,mockdb):
        if os.path.isfile(self.complete_file):
            pass
        else:
            return False
        store_snp_stats_in_db(self)
        if self.percentage_concordance > config.get('Concordance','threshold'):
            return True
        #If the concordance is below the threshold, we need to conduct a concordance search against the database
        #First we split the search across processors
        if self.search_key is None:
            sample = mockdb['Sample'].objects[self.sample_key]
            concord_search = mockdb['ConcordanceSearch'].__new__(config,sample=sample,snp_stats=self)
            self.search_key = concord_search.key
            concord_search.__launch_split_searches__(config)
            return False
        concord_search = mockdb['ConcordanceSearch'].objects[self.search_key]
        if concord_search.__is_complete__(config):
            return True
        #Now we gather
        if concord_search.__are_split_searches_complete__(config):
            if os.path.isfile(concord_search.qsub_file):
                return False
            concord_search.__fill_qsub_file__(config)
            concord_search.__launch__(config)
            return False
        return False

class ConcordanceSearch(QsubProcess):

    def __init__(self,config,key=int(-1),sample=None,snp_stats=None,output_filename=None,process_name='concord_search',**kwargs):
        if sample is None:
            sample = Sample(config,key="dummy_sample_key")
        if snp_stats is None:
            snp_stats = SnpStats(config,key=int(-1))
        if snp_stats.__class__.__name__ != "SnpStats":
            raise Exception("Trying to start a concordance search process for a non-snp_stats process.")
        input_dir = snp_stats.output_dir
        output_dir = input_dir
        QsubProcess.__init__(self,config,key=key,sample=sample,input_dir=input_dir,output_dir=output_dir,process_name=process_name,**kwargs)
        self.snp_path = snp_stats.snp_path
        self.first_match = sample.key
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

    def __fill_sub_qsub_files__(self,config):
        template_file= os.path.join(config.get('Common_directories','template'),config.get('Template_files','individual_search'))
        list_files = config.get('Concordance','split_lists').split(',')
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
            dictionary.update({'vcf_conversion_script':config.get('Concordance','vcf_conversion_script')})
            dictionary.update({'filter_file':config.get('Filenames','snp_filter_file')})
            with open(qsub_file,'w') as f:
                f.write(fill_template(template_file,dictionary))

    def __fill_qsub_file__(self,config):
        template_file= os.path.join(config.get('Common_directories','template'),config.get('Template_files','concord_search'))
        list_files = config.get('Concordance','split_lists').split(',')
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

    def __launch_split_searches__(self,config):
        self.__fill_sub_qsub_files__(config)
        list_files = config.get('Concordance','split_lists').split(',')
        for list_file in list_files:
            name = re.sub('.ls','',os.path.basename(list_file))
            qsub_file = self.sub_qsub_file_front + name + '.sh'
            self.__launch__(config,qsub_file=qsub_file)

    def  __are_split_searches_complete__(self,config):
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

    def __is_complete__(self,config):
        if os.path.isfile(self.complete_file):
            pass
        else:
            return False
        return_vals = grab_search_stats(self.output_path)
        self.first_match = return_vals[-2]
        self.first_concordance = return_vals[-1]
        self.second_match = return_vals[-4]
        self.second_concordance = return_vals[-3]
        self.third_match = return_vals[-6]
        self.third_concordance = return_vals[-5]
        self.fourth_match = return_vals[-8]
        self.fourth_concordance = return_vals[-7]
        self.fifth_match = return_vals[-10]
        self.fifth_concordance = return_vals[-9]
        return True
