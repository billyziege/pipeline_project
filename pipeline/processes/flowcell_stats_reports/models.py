import datetime
import os
import sys
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
from sge_email.scripts import send_email
from reports.post_pipeline_report import produce_outlier_table
from template.scripts import fill_template
from reports.pdf_wrapping import outlier_table_for_pdf, add_square_images, initialize_standard_doc

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
            seq_run = SequencingRun(config,key=-1)
        GenericProcess.__init__(self,config,key=key,process_name=process_name,**kwargs)
        if base_output_dir == None:
            self.base_output_dir = config.get('Common_directories','flowcell_reports')
        else:
            self.base_output_dir = base_output_dir
        self.flowcell_key = flowcell.key
        self.sequencing_run_key = seq_run.key
        self.sequencing_run_type = seq_run.run_type
        self.pipelines = None
        numbers = config.get('Flowcell_reports','numbers').split(',')
        for number in numbers:
            setattr(self,'flowcell_report_' + str(number) + '_key',None)
        self.state = 'Running'

    def __add_pipeline__(self,pipeline):
        """
        Connects the report with a pipeline by recoding the
        pipeline key and pipeline obj_type in a string.
        """
        if not re.search('Pipeline',pipeline.obj_type):
            raise Exception("Trying to add non-pipeline key to flowcell statistics reports")
        if not self.pipelines is None:
            self.pipelines += ';'
            self.pipelines += str(pipeline.key) + ":" + pipeline.obj_type
        else:
            self.pipelines = str(pipeline.key) + ":" + pipeline.obj_type

    def __current_pipeline_list__(self,mockdb):
        """
        Uses the pipelines string stored in self to generate
        a list of pipeline objects.
        """
        pipelines = []
        if self.pipelines is None:
            return pipelines
        pipelines_dict = self.pipelines.split(';')
        for d in pipelines_dict:
            pipeline_key, obj_type = d.split(':')
            try:
		pipeline = mockdb[obj_type].objects[int(pipeline_key)]
            except KeyError:
                sys.exit("Key error in determining pipeline for report.\n")
            pipelines.append(pipeline)
        return pipelines

    def __completed_samples_list__(self,mockdb):
        """
        Returns a list of sample keys associated
        with pipelines that have completed.
        """
        sample_keys = []
        for pipeline in self.__current_pipeline_list__(mockdb):
            if pipeline.__is_complete__():
                sample_keys.append(pipeline.sample_key)
        return sample_keys

    def __is_complete__(self,config,mockdb):
        """
        Return True if all pipelines in the report object
        have completed.
        """
        if GenericProcess.__is_complete__(self):
            return True
        if self.pipelines is None:
            return False
        for pipeline in self.__current_pipeline_list__(mockdb):
            if not pipeline.__is_complete__():
                return False
        #Add samples to the all sample list
        sample_keys = self.__completed_samples_list__(mockdb)
        write_list_file(sample_keys,config.get('Filenames','all_samples'),original_list_file=config.get('Filenames','all_samples'))
        return True

    def __generate_reports__(self,config,mockdb):
        """
        Checks the number of completed samples and generates reports
        based on this number and what has been previously reported.
        Return True only if a new report object is initialized.
        """
        sample_keys = self.__completed_samples_list__(mockdb)
        numbers = config.get('Flowcell_reports','numbers').split(',')
        numbers.sort(key=int,reverse=True)
        flowcell = mockdb['Flowcell'].__get__(config,key=self.flowcell_key)
        for number in numbers:
            if len(sample_keys) >= int(number):
                if getattr(self,'flowcell_report_' + str(number) + '_key') is None:
                    report = mockdb['FlowcellStatisticReport'].__new__(config,sample_keys=sample_keys,flowcell=flowcell,number=number,base_output_dir=self.base_output_dir)
                    report.__fill_qsub_file__(config)
                    report.__launch__(config)
                    setattr(self,'flowcell_report_' + str(number) + '_key',report.key)
                    return True
                return False
        return False

    def __send_reports__(self,config,mockdb):
        """
        For reports that have generated but not been sent,
        this script attaches the appropriate plots and tables
        and sends the email.
        """
        numbers = config.get('Flowcell_reports','numbers').split(',')
        for number in numbers:
            flowcell_report_key = getattr(self,'flowcell_report_' + str(number) + '_key')
            if flowcell_report_key is None:
                continue
            report = mockdb['FlowcellStatisticReport'].objects[flowcell_report_key]
            if report.report_sent is True: #If the report is already sent, next.
                continue
            if not report.__is_complete__(): #If the qsub script is still running, next.
                continue
            if self.sequencing_run_type == 'RapidRun' and str(number) == '16':
                recipients = config.get('Flowcell_reports','last_recipients')
                subject, body = report.__generate_flowcell_report_text__(config,mockdb,report_type="last_report")
                self.__finish__()
            elif self.sequencing_run_type == 'HighThroughputRun' and str(number) == '64':
                recipients = config.get('Flowcell_reports','last_recipients')
                subject, body = report.__generate_flowcell_report_text__(config,mockdb,report_type="last_report")
                self.__finish__()
            else:
                recipients = config.get('Flowcell_reports','subset_recipients')
                subject, body = report.__generate_flowcell_report_text__(config,mockdb,report_type="subset_report")
            files = []
            files.append(report.report_pdf)
            files.append(report.full_report)
            files.append(report.current_report)
            send_email(subject,body,recipients=recipients,files=files)
            report.__finish__()
            report.report_sent = True
        return 1



class FlowcellStatisticReport(QsubProcess):
    """
    Qsub process that generates the report files and sends an email.
    """

    def __init__(self,config,sample_keys=None,number=None,key=int(-1),flowcell=None,input_dir=None,base_output_dir=None,output_dir=None,date=strftime("%Y%m%d",localtime()),time=strftime("%H:%M:%S",localtime()),process_name='flowcell_report',complete_file=None,**kwargs):
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
        if sample_keys is None:
            self.sample_keys = ""
        else:
            self.sample_keys = ";".join(sample_keys)
        self.number = number
        #List of samples from the project
        self.all_samples_file = os.path.join(self.output_dir,'all_samples.ls')
        if self.key != -1:
            write_list_file(sample_keys,self.all_samples_file,original_list_file=config.get('Filenames','all_samples'))
        self.current_samples_file = os.path.join(self.output_dir,'current_samples.ls')
        if self.key != -1:
            write_list_file(sample_keys,self.current_samples_file)
        #Output files
        self.full_report = os.path.join(self.output_dir,'all_samples_report.csv')
        self.current_report = os.path.join(self.output_dir,'current_samples_report.csv')
        self.concordance_jpeg = os.path.join(self.output_dir,'concordance_vs_depth.jpeg')
        self.dbsnp_jpeg = os.path.join(self.output_dir,'dbsnp_vs_depth.jpeg')
        self.greater_than_10x_jpeg = os.path.join(self.output_dir,'greater_than_10x_vs_depth.jpeg')
        self.zero_coverage_jpeg = os.path.join(self.output_dir,'zero_coverage_vs_depth.jpeg')
        self.hethomratio_jpeg = os.path.join(self.output_dir,'hethomratio_vs_depth.jpeg')
        self.report_pdf = os.path.join(self.output_dir,self.flowcell_key + '_report.pdf')
        #Flag to keep track if report has been sent
        self.report_sent = False

    def __fill_qsub_file__(self,config):
        """
        Fills the qsub file from a template.  Since not all information is archived in the parent object, 
        the function also gets additional information on the fly for the qsub file.
        """
        template_file= os.path.join(config.get('Common_directories','template'),'flowcell_report.template')
        dictionary = {}
        for k,v in self.__dict__.iteritems():
            dictionary.update({k:str(v)})
        dictionary.update({'post_pipeline':config.get('Db_reports','post_pipeline')})
        dictionary.update({'concord_script':config.get('Flowcell_reports','concord_script')})
        dictionary.update({'dbsnp_script':config.get('Flowcell_reports','dbsnp_script')})
        dictionary.update({'tenx_script':config.get('Flowcell_reports','tenx_script')})
        dictionary.update({'zero_script':config.get('Flowcell_reports','zero_script')})
        dictionary.update({'hethom_script':config.get('Flowcell_reports','hethom_script')})
        with open(self.qsub_file,'w') as f:
            f.write(fill_template(template_file,dictionary))
    
    def __generate_flowcell_report_text__(self,config,mockdb,report_type="subset_report"):
        """
        Creates the outlier table, saves it to a file, and fills the
        subject and body of the report.
        """
        dictionary = {}
        for k,v in self.__dict__.iteritems():
            dictionary.update({k:str(v)})
        pdf_report = initialize_standard_doc(self.report_pdf)
        pdf_elements = []
        outlier_table = produce_outlier_table(config,mockdb,self.current_report) + "\n"
        if outlier_table is None:
            template_subject = os.path.join(config.get('Common_directories','template'),config.get('Flowcell_reports_email_templates',report_type + '_subject'))
            template_body = os.path.join(config.get('Common_directories','template'),config.get('Flowcell_reports_email_templates',report_type + '_no_outliers_body'))
        else:
            outlier_table_for_pdf(config,mockdb,pdf_elements,self.current_report)
            template_subject = os.path.join(config.get('Common_directories','template'),config.get('Flowcell_reports_email_templates',report_type + '_subject'))
            template_body = os.path.join(config.get('Common_directories','template'),config.get('Flowcell_reports_email_templates',report_type + '_body'))
        image_files = []
        image_files.append(self.concordance_jpeg)
        image_files.append(self.hethomratio_jpeg)
        image_files.append(self.dbsnp_jpeg)
        image_files.append(self.greater_than_10x_jpeg)
        image_files.append(self.zero_coverage_jpeg)
        pdf_elements.extend(add_square_images(image_files))
        pdf_report.build(pdf_elements)
        sample_keys = self.sample_keys.split(";")
        number_samples = len(sample_keys)
        dictionary.update({'number_samples': str(number_samples)})
        subject = fill_template(template_subject,dictionary)
        body = fill_template(template_body,dictionary)
        return subject, body

