import os
import re
import csv
import shutil
from time import strftime, localtime
from config.scripts import MyConfigParser
from demultiplex_stats.fill_demultiplex_stats import fill_demultiplex_stats
from physical_objects.hiseq.models import Flowcell, HiSeqMachine
from processes.models import GenericProcess, StandardPipeline, QsubProcess
from processes.parsing import parse_sample_sheet
from processes.hiseq.sample_sheet import clean_sample_name, SampleSheetObjList
from processes.hiseq.sample_sheet import SampleSheetFormatException, send_missing_sample_sheet_email
from processes.hiseq.scripts import list_project_sample_dirs, list_sample_dirs
from processes.hiseq.merge_casava_results import merge_split_casava_results
from sge_email.scripts import send_email
from template.scripts import fill_template
# Create your models here.

class SequencingRun(GenericProcess):
    """
    This object manages and stores information about a sequencing run.  It is currently written to catch the results of casava and 
    to search the hiseq data directory for additional information.
    """

    def __init__(self,config,key=int(-1),input_dir=None,flowcell=None,machine=None,date='dummy',run_number='dummy',side='dummy',operator=None,run_type=None,process_name='sequencing_run',no_delete=False,**kwargs):
        """
        Initializes the object.
        """
        if not input_dir is None:
            GenericProcess.__init__(self,config,key=key,**kwargs)
            self.input_dir = input_dir
            self.flowcell_key = flowcell.key
            self.machine_key = machine.key
            self.date = date
            self.run_number = run_number
            self.side = side
            self.state = "Running"
            self.operator = operator
            self.run_type = run_type
            self.bcltofastq_pipeline_key = None
            #self.input_amount = input_amount
            #self.yield_from_library = yield_from_library
            #self.average_bp = average_bp
            output_name_pieces = []
            output_name_pieces.append(str(date))
            output_name_pieces.append(str(machine.key))
            output_name_pieces.append(str(run_number))
            output_name_pieces.append(str(side)+str(flowcell.key))
            output_name = "_".join(output_name_pieces)
            self.output_dir = os.path.join(config.get('Common_directories','hiseq_output'),output_name)
            self.complete_file = os.path.join(config.get('Common_directories','hiseq_output'),output_name+"/"+config.get('Filenames','bcls_ready'))
            self.no_delete = no_delete
            self.interop_archived = "False"

    def __start_bcltofastq_pipeline__(self,configs,mockdb):
        """
        Launches a bunch of processes that will first convert bcl to fastq
        and then in turn launch other pipelines and do some qc.
        """
        if configs["system"].get("Logging","debug") is "True":
            print "Starting the BclToFastqPipeline for sequencing run " + str(self.key) + " with flowcell " + str(self.flowcell_key)
        bcl2fastq_pipeline = mockdb['BclToFastqPipeline'].__new__(configs['system'],seq_run=self)
        self.bcltofastq_pipeline_key = bcl2fastq_pipeline.key
        bcl2fastq_pipeline.state = "Running"

    def __launch_illuminate__(self,configs,mockdb):
        """
        Collects the stats from the InterOp directory.
        """
        illuminate = mockdb['Illuminate'].__new__(configs['system'],seq_run=self)
        illuminate.__fill_qsub_file__(configs,template_config=configs["seq_run"])
        self.illuminate_key = illuminate.key
        illuminate.__launch__(configs['system'])

    def __launch_archive_fastq__(self,configs,mockdb):
        """
        This launches the process that will archive the fastq directories.
        """
        bcl2fastq_pipeline = mockdb['BclToFastqPipeline'].__get__(configs['system'],self.bcltofastq_pipeline_key)
        output_name = os.path.basename(self.output_dir)
        output_dir = os.path.join(configs["system"].get('Common_directories','casava_output'),output_name)
        input_dir = os.path.join(bcl2fastq_pipeline.output_dir,output_name)
        generic_copy = mockdb['GenericCopy'].__new__(configs['system'],input_dir=input_dir,output_dir=output_dir)
        self.generic_copy_key = generic_copy.key
        generic_copy.__fill_qsub_file__(configs,template_config=configs["seq_run"])
        generic_copy.__launch__(configs['system'])

    def __launch_clean__(self,configs,mockdb):
        bcl2fastq_pipeline = mockdb['BclToFastqPipeline'].__get__(configs['system'],self.bcltofastq_pipeline_key)
        output_name = os.path.basename(self.output_dir)
        output_dir = os.path.join(configs["system"].get('Common_directories','casava_output'),output_name)
        clean = mockdb['GenericClean'].__new__(configs['system'],rm_dir=bcl2fastq_pipeline.output_dir,output_dir=output_dir)
        self.generic_clean_key = clean.key
        clean.__fill_qsub_file__(configs,template_config=configs["seq_run"])
        clean.__launch__(configs['system'])

    def __archive_sequencing_run_data__(self,configs,input_dir,output_dir):
        """
        This archives the pertinent sequencing run data (a small amount, 
        which is why it is not delegated to qsub) that comes directly
        from the HiSeq machines.
        """
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        missing_paths = []
        sub_dirs = ["InterOp"]
        self.flowcell_content_found = True
        if not hasattr(self,"flowcell_content_reported") or self.flowcell_content_reported is None:
            self.flowcell_content_reported = False
        for sub_dir in sub_dirs:
            if os.path.isdir(os.path.join(input_dir,sub_dir)):
                if not os.path.exists(os.path.join(output_dir,sub_dir)):
                    shutil.copytree(os.path.join(input_dir,sub_dir),os.path.join(output_dir,sub_dir))
            else:
                missing_paths.append(os.path.join(input_dir,sub_dir))
                self.flowcell_content_found = False
        files = ["First_Base_Report.htm","RunInfo.xml","runParameters.xml"]
        for file in files:
            if os.path.isfile(os.path.join(input_dir,file)):
                shutil.copy(os.path.join(input_dir,file),output_dir)
            else:
                missing_paths.append(os.path.join(input_dir,file))
                self.flowcell_content_found = False
        if not self.flowcell_content_found:
            if not self.flowcell_content_reported:        
                message = "The flowcell "+self.flowcell_key+" has finished copying over but is missing the following paths:\n"
                message += "\n".join(missing_paths)
                message += "\nPlease check.\n\n"
                send_email("Missing flowcell data.",message,recipients='zerbeb@humgen.ucsf.edu')  
                #send_email("Missing flowcell data.",message,recipients='zerbeb@humgen.ucsf.edu,LaoR@humgen.ucsf.edu')  
                self.flowcell_content_reported = True
            return False
        return True
        
    def __link_to_web_portal__(self,config):
        """
        Automatically adds the project info to the link file.
        """
        path = config.get('Filenames','web_portal_link_file')
        with open(path,'r+') as f:
            content = f.read()
            f.seek(0,0)
            for directory in os.listdir(self.output_dir):
                if not directory.beginswith('Project_'):
                    continue
                if not os.path.isdir(directory):
                    continue
                pieces = directory.split("_")
                if len(pieces) < 2:
                    continue
                output = [pieces[1]]
                output.append(os.path.join(self.output_dir,directory))
                output.append(self.flowcell+"_"+directory)
                f.write("\t".join(output)+"\n")
            f.write(content)
              
    def __is_complete__(self,configs,mockdb,*args,**kwargs):
        """
        Due to the inclusion of sub-processes (bclto fastq pipeline, illuminate, and launched pipelines),
        this function contains the logic to check to makes sure all of these processes
        have completed successfully.
        """
        if configs["system"].get("Logging","debug") is "True":
            print "Checking to see if seq run is complete (and advancing post-seq run pipeline"
        if GenericProcess.__is_complete__(self,*args,**kwargs):
            return True
        if not os.path.isfile(self.complete_file):
            return False
        print self.complete_file
        if not self.interop_archived is True:
            output_name = os.path.basename(self.output_dir)
            if not self.__archive_sequencing_run_data__(configs,self.output_dir,os.path.join(configs["system"].get('Common_directories','hiseq_run_log'),output_name)):
                return False
        if not hasattr(self,"bcltofastq_pipeline_key") or self.bcltofastq_pipeline_key is None or not hasattr(self,"illuminate_key") or self.illuminate_key is None:
            if not hasattr(self,"bcltofastq_pipeline_key") or self.bcltofastq_pipeline_key is None:
                self.__start_bcltofastq_pipeline__(configs,mockdb)
            if not hasattr(self,"illuminate_key") or self.illuminate_key is None:
                self.__launch_illuminate__(configs,mockdb)
            return False
        illuminate = mockdb['Illuminate'].__get__(configs['system'],self.illuminate_key)
        if not illuminate.__is_complete__(configs,mockdb=mockdb,*args,**kwargs):
            if configs["system"].get("Logging","debug") is "True":
                print "Waiting on illuminate"
            return False
        bcl2fastq_pipeline = mockdb['BclToFastqPipeline'].__get__(configs['system'],self.bcltofastq_pipeline_key)
        if not bcl2fastq_pipeline.__is_complete__(configs,mockdb=mockdb,*args,**kwargs):
            return False
        if not hasattr(self,"generic_copy_key") or self.generic_copy_key is None:
            self.__launch_archive_fastq__(configs,mockdb)
            return False
        archive = mockdb['GenericCopy'].__get__(configs['system'],self.generic_copy_key)
        if archive.__is_complete__(*args,**kwargs):
            if not hasattr(self,"generic_clean_key") or self.generic_clean_key is None:
                self.__launch_clean__(configs,mockdb)
            #    self.__link_to_web_portal__(configs['system'])
                if configs["system"].get("Logging","debug") is "True":
                    print "  Filling stats"
                flowcell = mockdb['Flowcell'].__get__(configs['system'],self.flowcell_key)
                machine = mockdb['HiSeqMachine'].__get__(configs['system'],self.machine_key)
                fill_demultiplex_stats(configs['system'],mockdb,self.output_dir,flowcell,machine)
                return False
        clean = mockdb['GenericClean'].__get__(configs['system'],self.generic_clean_key)
        if clean.__is_complete__(*args,**kwargs):
            self.__finish__(*args,**kwargs)
            return True
        return False
        
class Illuminate(QsubProcess):
    """
    Keeps track of the Illuminate process.
    """

    def __init__(self,config,key=-1,seq_run=None,**kwargs):
        if not seq_run is None:
            self.flowcell_key = seq_run.flowcell_key
            input_dir = seq_run.output_dir
            output_dir = os.path.join(config.get("Common_directories","hiseq_run_log"),os.path.basename(seq_run.output_dir))
            QsubProcess.__init__(self,config,key=key,output_dir=output_dir,input_dir=input_dir,process_name="illuminate",**kwargs)
            self.run_qc_metrics_file = os.path.join(output_dir,config.get("Filenames","run_qc_metrics"))

class Casava(QsubProcess):
    """
    Keeps track of the casava process and does any needed processing.
    """

    def __init__(self,config,key=-1,prev_step=None,pipeline=None,split_by_lane=True,split_by_index_length=True,process_name="casava",**kwargs):
        """
        In addition to initializing, other steps are completed.  These are commented below.
        """
        if not prev_step is None:
            input_dir = os.path.join(pipeline.output_dir,"Data/Intensities/BaseCalls")
            output_dir = os.path.join(pipeline.output_dir,os.path.basename(pipeline.output_dir))
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)
            original_sample_sheet_file = os.path.join(pipeline.input_dir,"SampleSheet.csv")
            if not os.path.isfile(original_sample_sheet_file):#Check to make sure original sample sheet exists
               send_missing_sample_sheet_email(original_sample_sheet_file)
               raise SampleSheetFormatException("No sample sheet found: "+str(original_sample_sheet_file))
            sample_sheet_obj_list = SampleSheetObjList(sample_sheet_file=original_sample_sheet_file)
            sample_sheet_obj_list.list[0].sample_sheet_table.__write_file__(os.path.join(output_dir,"SampleSheet.csv"))#Copy sample sheet to final output dir.
            self.merged = True
            split_categories = []
            self.split_by_lane = split_by_lane
            if split_by_lane is True: #Split by lane (speed up especially for high throughput)
                sample_sheet_obj_list = sample_sheet_obj_list.__partition_sample_sheet_objects__("Lane")
                split_categories.append("Lane")
                self.merged = False
            self.split_by_index_length = split_by_index_length
            if split_by_index_length == True: #Split by index lane (prevents casava from breaking when pool samples have different index lengths)
                for sample_sheet_obj in sample_sheet_obj_list.list:
                    sample_sheet_obj.__attach_max_column_number__("Index")
                sample_sheet_obj_list = sample_sheet_obj_list.__partition_sample_sheet_objects__("Index",use_length=True)
                split_categories.append("Index_length")
                self.merged = False
            number_tasks = len(sample_sheet_obj_list.list)
            temporary_output_directories = sample_sheet_obj_list.__create_meta_directories_and_write_files__(os.path.join(output_dir,"split"),split_categories)
            self.temporary_output_dir = ":".join(temporary_output_directories)
            sample_sheets = [os.path.join(d,"SampleSheet.csv") for d in temporary_output_directories]
            self.sample_sheet = ":".join(sample_sheets)
            sample_sheet_obj_list.__attach_masks__(run_parameters_path=os.path.join(pipeline.input_dir,"runParameters.xml"))
            masks = []
            for sample_sheet_obj in sample_sheet_obj_list.list:
                mask = sample_sheet_obj.__get_meta_datum__("mask")
                mask, number = re.subn(',','-',mask)
                masks.append(mask)
            self.mask = ":".join(masks)
            QsubProcess.__init__(self,config,key=key,output_dir=output_dir,input_dir=input_dir,number_tasks=number_tasks,process_name=process_name,**kwargs)
            self.flowcell_key = pipeline.flowcell_key
            self.seq_run_key = pipeline.seq_run_key
 
    def __push_samples_into_relevant_pipelines__(self,configs,mockdb):
        """
        Provides the interface from which all post casava pipelines are run.
        """
        if configs["system"].get("Logging","debug") is "True":
            print "  Starting post casava pipelines for " + self.flowcell_key
            print "  Determining Sample dirs"
        sample_dirs = list_sample_dirs(self.output_dir.split(":"))
        if configs["system"].get("Logging","debug") is "True":
           print "  Samples: " + str(sample_dirs) 
        flowcell_dir_name = os.path.basename(self.output_dir)
        automation_parameters_config = MyConfigParser()
        automation_parameters_config.read(configs["system"].get("Filenames","automation_config"))
        for sample in sample_dirs:
            if configs["system"].get("Logging","debug") is "True":
               print "    Processing " + sample
            #running_location = identify_running_location_with_most_currently_available(configs,storage_devices)
            running_location = "Speed"
            parsed = parse_sample_sheet(configs['system'],mockdb,sample_dirs[sample][0])
            description_pieces = parsed['description'].split('-')
            pipeline_key = description_pieces[-1]
            pipeline_name = automation_parameters_config.safe_get("Pipeline",pipeline_key)
            mockdb["FastQCPipeline"].__new__(configs['system'],input_dir=sample_dirs[sample][0],flowcell_dir_name=flowcell_dir_name,project=parsed['project_name'],**parsed)
            if pipeline_name is None:
                continue
            mockdb[pipeline_name].__new__(configs['system'],input_dir=sample_dirs[sample][0],pipeline_key=pipeline_key,seq_run_key=self.seq_run_key,project=parsed['project_name'],flowcell_dir_name=flowcell_dir_name,**parsed)

    def __do_all_relevant_pipelines_have_first_step_complete__(self,configs,mockdb):
        """
        Since the first step of post casava pipelines is to copy the data,
        moving the data after these pipelines are started must wait for
        this step to complete.  This is only a concern when everything is 
        automated.  This checks that step and whether the FastQCPipeline is finished.
        """
        pipeline_names = configs["system"].get('Pipeline','post_casava_automated').split(',')
        for pipeline_name in pipeline_names:
                try:
                    seq_run_key_dict = mockdb[pipeline_name].__attribute_value_to_object_dict__('seq_run_key')
                    pipeline_config = MyConfigParser()
                    pipeline_config.read(config.get('Pipeline',pipeline_name))
                    for pipeline in seq_run_key_dict[self.seq_run_key]:
                        if pipeline_name == "FastQCPipleine":
                            if not pipeline.__is_complete__():
                                return False
                        if not pipeline.__check_first_step__(pipeline_config):
                            return False
                except:
                    continue
        return True

    def __is_complete__(self,configs,mockdb,*args,**kwargs):
        """
        Handles the merging of casava directories run in parallel and launching pipelines as well as the normal check.
        """
        if GenericProcess.__is_complete__(self,*args,**kwargs):
            return True
        for complete_file in self.complete_file.split(":"):
            if not os.path.isfile(complete_file):
                return False
        ##Handle merging
        split_categories = []
        if self.merged is False:
            if configs["system"].get("Logging","debug") is "True":
                print "Merging casava results for " + self.flowcell_key
            if self.split_by_index_length is True:
                split_categories.append("Index_length")
            if self.split_by_lane is True:
                split_categories.append("Lane")
            split_dir = os.path.join(self.output_dir,"split")
            merge_split_casava_results(split_dir,self.output_dir,split_categories)
            #exit("Just merged.  Stopping.")
            shutil.rmtree(split_dir)
            self.merged = True
        ##Launch pipelines
        self.__push_samples_into_relevant_pipelines__(configs,mockdb)
        self.__finish__(*args,**kwargs)
        return True

    def __fill_qsub_file__(self,configs,template_config=None):
        """
        Simply fills process_name template with appropriate info. Needed to detangle the masks.
        """
        if configs["system"].get("Logging","debug") is "True":
            print "Trying to fill " + self.qsub_file
        if template_config is None:
            print configs['system'].get('Common_directories','template')
            print self.process_name
            print configs['pipeline'].get('Template_files',self.process_name)
            template_file= os.path.join(configs['system'].get('Common_directories','template'),configs['pipeline'].get('Template_files',self.process_name))
        else:
            template_file= os.path.join(configs['system'].get('Common_directories','template'),template_config.get('Template_files',self.process_name))
        if configs["system"].get("Logging","debug") is "True":
           print  "Template file " + template_file
        with open(self.qsub_file,'w') as f:
            dictionary = {}
            for key, value in self.__dict__.iteritems():
                if key == 'mask':
                    value, number = re.subn('-',',',value)
                dictionary[key] = value
            if configs["system"].get("Logging","debug") is "True":
                print "Filling qsub with " + str(dictionary)
            f.write(fill_template(template_file,dictionary))
                    
class IndexReport(QsubProcess):
    """
    Keeps track of the Index report process.
    """

    def __init__(self,config,key=int(-1),pipeline_config=None,prev_step=None,process_name='index_report',pipeline=None,**kwargs):
        if not prev_step is None:
            self.flowcell_key = pipeline.flowcell_key
            output_dir = os.path.join(prev_step.output_dir,"Undetermined_indices")
            QsubProcess.__init__(self,config,key=key,output_dir=output_dir,input_dir="None",process_name=process_name,**kwargs)

                
class Md5CheckSum(QsubProcess):
    """
    Keeps track of the md5 process.
    """

    def __init__(self,config,key=int(-1),pipeline_config=None,prev_step=None,process_name='md5_fastq',pipeline=None,**kwargs):
        if not prev_step is None:
            self.flowcell_key = pipeline.flowcell_key
            output_dir = prev_step.input_dir
            QsubProcess.__init__(self,config,key=key,output_dir=pipeline.input_dir,input_dir=pipeline.input_dir,process_name="md5_fastq",**kwargs)

                
            
            
       
#Am considering adding the run_types as separate classes.
#class HighThroughputRun(SequencingRun):
#    """
#    Sequencing run with 8 lanes completed in 11 days.
#    """
#    pass

#class RapidRun(SequencingRun):
#    """
#    Sequencing run with 2 lanes completed in 27 hours.
#    """
#    pass
