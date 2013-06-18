import datetime
import os
import pytz
import subprocess
import re
from time import strftime, localtime
from mockdb.models import NumberedObject
from physical_objects.hiseq.models import Flowcell
from processes.models import GenericProcess, QsubProcess
from processes.hiseq.models import SequencingRun
from sge_queries.nodes import grab_good_node
from sge_queries.jobs import check_if_single_job_running_on_system
from processes.flowcell_stats_reports.scripts import write_list_file

class FlowcellStatisticsReports(GenericProcess):
    """
    Object to keep track of samples that come out of a flowcell
    from a sequencing run.  This object automatically sends reports
    when 1, 4, and 16 (and also 32 and 64 for HighThroughputRun) 
    samples have completed.
    """
    def __init__(self,config,key=int(-1),flowcell=None,seq_run=None,base_output_dir=None,process_name='flowcell_reports',**kwargs):
        """
        Initiates the report object attached to the flowcell and sequencing run
        but not attached to any pipelines as of yet.
        """
        if flowcell is None:
            flowcell = Flowcell(config,key="dummy_flowcell_key")
        if flowcell.__class__.__name__ != "Flowcell":
            raise Exception("Trying to start a flowcell statistics reports object on a non-flowcell.")
        if seq_run is None:
            pipeline = SequencingRun(config,key="dummy_seq_run_key")
        GenericProcess.__init__(self,config,key=key,process_name=process_name,**kwargs)
        if base_output_dir == None:
            base_output_dir = config.get('Common_directories','flowcell_reports')
        else:
            self.base_output_dir = base_output_dir
        self.flowcell_key = flowcell.key
        self.sequencing_run_key = seq_run.key
        self.sequencing_run_type = seq_run.run_type
        self.pipelines = None
        self.flowcell_report_1_key = None
        self.flowcell_report_4_key = None
        self.flowcell_report_16_key = None
        self.flowcell_report_32_key = None
        self.flowcell_report_64_key = None
        self.state = 'Running'

    def __add_pipeline__(self,pipeline):
        """
        Connects the report with a pipeline by recoding the
        pipeline key and pipeline obj_type in a string.
        """
        if not res.search('Pipeline',pipeline):
            raise Exception("Trying to add non-pipeline key to flowcell statistics reports")
        if not self.pipelines is None:
            self.pipelines += ';'
            self.pipelines += pipeline.key + ":" + pipeline.__class__.__name__
        else:
            self.pipelines = pipeline.key + ":" + pipeline.obj_type

    def __current_pipeline_list__(self,mockdb):
        """
        Uses the pipelines string stored in self to generate
        a list of pipeline objects.
        """
        pipelines = []
        if self.pipelines is None:
            return pipelines
        pipelines_dict = self.pipeline_keys.split(';')
        for d in pipelines_dict:
            pipeline_key, obj_type = d.split(':')
            pipeline = mockdb[obj_type].objects[pipeline_key]
            pipelines.addend(pipeline)
        return pipelines

    def __completed_samples_list__(self,mockdb):
        """
        Returns a list of sample keys associated
        with pipelines that have completed.
        """
        sample_keys = []
        for pipeline in self.__current_pipeline_list__(mockdb):
            if pipeline.__is_complete__():
                sample_keys.addend(pipeline.sample_key)
        return sample_keys

    def __is_complete__(self,mockdb):
        """
        Return True if all pipelines in the report object
        have completed.
        """
        if self.pipelines is None:
            return False
        for pipeline in self.__current_pipeline_list__(mockdb):
            if not pipeline.__is_complete__():
                return False
        #Add samples to the all sample list
        sample_keys = self.__complete_samples_list__(mockdb)
        write_list_file(sample_keys,config.get('Filenames','all_samples'),original_list_file=config.get('Filenames','all_samples'))
        return True

    def __generate_reports__(self,config,mockdb):
        """
        Checks the number of completed samples and generates reports
        based on this number and what has been previously reported.
        Return True only if a new report object is initialized.
        """
        sample_keys = self.__complete_samples_list__(mockdb)
        numbers = config.get('Flowcell_reports','numbers').split(',')
        numbers.sort(key=int,reverse=True)
        for number in numbers:
            if len(sample_keys) >= number:
                if getattr(self,'flowcell_report_' + str(number) + '_key') is None:
                    report = mockdb['FlowcellStatisticReport'].__new__(config,sample_keys=sample_keys,number=number,base_output_dir=self.base_output_dir)
                    setattr(self,'flowcell_report_' + str(number) + '_key',report.key)
                    return True
                return False
        return False



class FlowcellStatisticReport(QsubProcess):
    """
    Qsub process that generates the report files and sends an email.
    """

    def __init__(self,config,sample_keys,number,key=int(-1),flowcell=None,input_dir=None,base_output_dir=None,output_dir=None,date=strftime("%Y%m%d",localtime()),time=strftime("%H:%M:%S",localtime()),process_name='flowcell_report',complete_file=None,**kwargs):
        """
        Initializes flowcell statistic report.
        """
        if flowcell is None:
            flowcell = Flowcell(config,key="dummy_flowcell_key")
        if flowcell.__class__.__name__ != "Flowcell":
            raise Exception("Trying to start a flowcell statistics reports object on a non-flowcell.")
        if output_dir is None:
            if base_output_dir is None:
                base_output_dir = config.get('Common_directories','flowcell_reports')
            self.output_dir = os.path.join(os.path.join(base_output_dir,flowcell.key + "_reports"),str(number))
        else:
            self.output_dir = output_dir
        if complete_file is None:
            self.complete_file = os.path.join(self.output_dir,"report_" + str(number) + ".complete")
        else:
            self.complete_file = complete_file
        QsubProcess.__init__(self,config,key=key,input_dir=input_dir,base_output_dir=base_output_dir,output_dir=self.output_dir,date=date,time=time,process_name=process_name,complete_file=self.complete_file,**kwargs)
        self.flowcell_key = flowcell.key
        self.sample_keys = ";".join(sample_keys)
        self.number = number
        #List of samples from the project
        self.all_samples_file = os.path.join(self.output_dir,'all_samples.ls')
        write_list_file(sample_keys,self.all_samples_file,original_list_file=config.get('Filenames','all_samples'))
        self.current_samples_file = os.path.join(self.output_dir,'current_samples.ls')
        write_list_file(sample_keys,self.current_samples_file)
        #Output files
        self.full_report = os.path.join(self.output_dir,'all_samples_report.csv')
        self.current_report = os.path.join(self.output_dir,'current_samples_report.csv')
        self.concordance_png = os.path.join(self.output_dir,'concordance_vs_depth.png')
        self.dbsnp_png = os.path.join(self.output_dir,'dbsnp_vs_depth.png')
        self.greater_than_10x_png = os.path.join(self.output_dir,'greater_than_10x_vs_depth.png')
        self.zero_coverage_png = os.path.join(self.output_dir,'zero_coverage_vs_depth.png')
        self.hethomratio_png = os.path.join(self.output_dir,'hethomratio_vs_depth.png')

    def __fill_qsub_file__(self,config):
        """
        Fills the qsub file from a template.  Since not all information is archived in the parent object, 
        the function also gets additional information on the fly for the qsub file.
        """
        template_file= os.path.join(config.get('Common_directories','template'),'flowcell_report.template')
        dictionary = {}
        for k,v in self.__dict__.iteritems():
            dictionary.update({k:str(v)})
        dictionary.update({'post_pipeline_report':config.get('Db_reports','post_pipeline')})
        dictionary.update({'concord_script':config.get('Flowcell_reports','concord_script')})
        dictionary.update({'dbsnp_script':config.get('Flowcell_reports','dbsnp_script')})
        dictionary.update({'tenx_script':config.get('Flowcell_reports','tenx_script')})
        dictionary.update({'zero_script':config.get('Flowcell_reports','zero_script')})
        dictionary.update({'hethom_script':config.get('Flowcell_reports','hethom_script')})
        with open(self.qsub_file,'w') as f:
            f.write(fill_template(template_file,dictionary))

