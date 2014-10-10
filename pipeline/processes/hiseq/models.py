import os
import csv
import shutil
from time import strftime, localtime
from config.scripts import safe_get
from physical_objects.hiseq.models import Flowcell, HiSeqMachine
from processes.models import GenericProcess, StandardPipeline
from process.hiseq.sample_sheet import clean_sample_name, copy_sample_sheet, SampleSheetObjList
from process.hiseq.sample_sheet import SampleSheetFormattingError, send_missing_sample_sheet_email
from process.hiseq.scripts import list_project_sample_dirs
# Create your models here.

class SequencingRun(GenericProcess):
    """
    This object manages and stores information about a sequencing run.  It is currently written to catch the results of casava and 
    to search the hiseq data directory for additional information.
    """

    def __init__(self,config,key=int(-1),flowcell=None,machine=None,date='dummy',run_number='dummy',side='dummy',operator=None,run_type=None,process_name='sequencing_run',no_delete=False,**kwargs):
        """
        Initializes the object.
        """
        if flowcell is None:
            flowcell = Flowcell(config,key="dummy_flowcell_key")
        if machine is None:
            machine = HiSeqMachine(config,key="dummy_machine_key")
        GenericProcess.__init__(self,config,key=key,**kwargs)
        self.begin_timestamp = begin_timestamp
        self.end_timestamp = end_timestamp
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
        output_name_pieces.append(str(side)+str(flowcell_key))
        output_name = "_".join(output_name_pieces)
        if output_dir == None:
            self.output_dir = os.path.join(config.get('Common_directories','hiseq_output'),output_name)
        else:
            self.output_dir = output_dir
        if complete_file == None:
            self.complete_file = os.path.join(config.get('Common_directories','hiseq_output'),config.get('Filenames','bcls_ready'))
        else:
            self.complete_file = complete_file
        self.no_delete = no_delete

    def __start_bcltofastq_pipeline__(self,configs,mockdb):
        """
        Launches a bunch of processes that will first convert bcl to fastq
        and then in turn launch other pipelines and do some qc.
        """
        bcl2fastq_pipeline = mockdb['BclToFastqPipeline'].__new__(configs['system'],seq_run=self)
        self.bcltofastq_pipeline_key = bcl2fastq_pipeline.key

    def __launch_illuminate__(self,configs,mockdb):
        """
        Collects the stats from the InterOp directory.
        """
        illuminate = mockdb['Illuminate'].__new__(configs['system'],seq_run=self)
        self.illuminate_key = illuminate.key
        illuminate.__fill_qsub_file__(configs)
        illuminate.__launch__(configs['system'])

    def __launch_archive_fastq__(self,configs,mockdb):
        """
        This launches the process that will archive the fastq directories.
        """
        bcl2fastq_pipeline = mockdb['BclToFastqPipeline'].__get__(configs['system'],self.bcltofastq_pipeline_key)
        gen_cp = mockdb['GenericCopy'].__new__(configs['system'],input_dir=bcl2fastq_pipeline.output_dir,output_dir=self.output_dir)
        self.archive_key = gen_cp.key
        gen_cp.__fill_qsub_file__(configs)
        gen_cp.__launch__(configs['system'])

    def __archive_sequencing_run_data__(self,input_dir,output_dir):
        """
        This archives the pertinent sequencing run data (a small amount, 
        which is why it is not delegated to qsub) that comes directly
        from the HiSeq machines.
        """
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        sub_dirs = ["InterOp"]
        for sub_dir in sub_dirs:
            shutil.copytree(os.path.join(input_dir,sub_dir),output_dir)
        files = ["First_Base_Report.htm","RunInfo.xml","runParameters.xml"]
        for file in files:
            shutil.copy(os.path.join(input_dir,file),output_dir)
        self.interop_archived = "True"
        
    def __link_to_web_portal__(self,config):
        """
        Automatically adds the project info to the link file.
        """
        path = config.get('Filenames','web_portal_link_file'))
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
        if GenericProcess.__is_complete__(self,*args,**kwargs):
            return True
        if not os.path.isfile(self.complete_file):
            return False
        if self.interop_archived != "True":
            self.__archive_sequencing_run_data__(self.output_dir,config.get('Common_directories','hiseq_archive'))
        if not hasattr(self,bcltofastq_pipeline_key) or self.bcltofastq_pipeline_key is None or not hasattr(self,illuminate_key) or self.illuminate_key is None:
            if not hasattr(self,"bcltofastq_pipeline_key") or self.bcltofastq_pipeline_key is None:
                self.__launch_bcltofastq_pipeline__(configs,mockdb)
            if not hasattr(self,"illuminate_key") or self.illuminate_key is None:
                self.__launch_illuminate__(configs,mockdb)
            return False
        bcl2fastq_pipeline = mockdb['BclToFastqPipeline'].__get__(configs['system'],self.bcltofastq_pipeline_key)
        if not bcl2fastq_pipeline.__is_complete__(configs,mockdb,*args,**kwargs):
            return False
        else:
            if not hasattr(self,"archive_key") or self.archive_key is None:
                self.__launch_archive_fastq__(configs,mockdb)
                return False
        archive = mockdb['GenericCopy'].__get__(configs['system'],self.archive_key)
        if archive.__is_complete__(*args,**kwargs):
            self.__link_to_web_portal__(configs['system'])
            if configs["system"].get("Logging","debug") is "True":
                print "  Flowcell " + flowcell.key
                print "  Machine " + machine.key
                print "  Filling stats"
            flowcell = mockdb['Flowcell'].__get__(configs['system'],self.flowcell_key)
            machine = mockdb['HiSeqMachine'].__get__(configs['system'],self.machine_key)
            fill_demultiplex_stats(configs['system'],mockdb,self.output_dir,flowcell,machine)
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
            self.run_qc_metrics_file = os.path.join(seq_run.output_dir,config.get("Filenames","run_qc_metrics"))
            input_dir = os.path.join(seq_run.input_dir,"InterOp")
            QsubProcess.__init__(self,config,key=key,output_dir=seq_run.output_dir,input_dir=seq_run.input_dir,process_name="illuminate",**kwargs)

class Casava(QsubProcess):
    """
    Keeps track of the casava process and does any needed processing.
    """

    def __init__(self,config,key=-1,prev_step=None,pipeline=None,split_by_lane="True",split_by_index_length="True",process_name="casava",**kwargs):
        """
        In addition to initializing, other steps are completed.  These are commented below.
        """
        if not prev_step is None:
            input_dir = prev_step.output_dir
            output_dir = os.path.join(input_dir,os.path.basename(pipeline.output_dir))
            original_sample_sheet_file = os.path.join(pipeline.input_dir,"SampleSheet.csv")
            if not os.path.isfile(original_sample_sheet_file):#Check to make sure original sample sheet exists
               send_missing_sample_sheet_email(original_sample_sheet_file)
               raise SampleSheetFormatException("No sample sheet found: "+str(original_sample_sheet_file))
            sample_sheet_obj_list = SampleSheetObjList(sample_sheet_file=original_sample_sheet_file)
            sample_sheet_obj_list[0].sample_sheet_table.__write_file__(os.path.join(pipeline.output_dir,"SampleSheet.csv"))#Copy sample sheet to final output dir.
            split_categories = []
            self.split_by_lane = split_by_lane
            if split_by_lane == "True": #Split by lane (speed up especially for high throughput)
                sample_sheet_obj_list = sample_sheet_obj_list.__partition_sample_sheet_objects__(self,"Lane")
                split_categories.append("Lane")
            self.split_by_index_length = split_by_index_legnth
            if split_by_index_length == "True": #Split by index lane (prevents casava from breaking when pool samples have different index lengths)
                for sample_sheet_obj in sample_sheet_obj_list.list:
                    sample_sheet_obj.__attach_max_column_number__("Index")
                sample_sheet_obj_list = sample_sheet_obj_list.__partition_sample_sheet_objects__(self,"Index",use_length=True)
                split_categories.append("Index_length")
            number_tasks = len(sample_sheet_obj_list)
            temporary_output_directories = sample_sheet_obj_list.__create_meta_directories_and_write_files__(os.path.join(output_dir,"split"),split_categories)
            self.temporary_output_dir = ":".join(temporary_output_directories)
            sample_sheets = [os.path.join(d,"SampleSheet.csv") for d in temporary_output_directories]
            self.sample_sheet = ":".join(sample_sheets)
            sample_sheet_obj_list.__attach_masks__(run_parameters_path=os.path.join(pipeline.input_dir,"runParameters.xml"))
            masks = []
            for sample_sheet_obj in sample_sheet_obj_list.list:
                mask = sample_sheet_obj.__get_meta_datum__("mask")
                masks.append(mask)
            self.mask = ":".join(masks)
            QsubProcess.__init__(self,config,key=key,output_dir=output_dir,input_dir=input_dir,number_tasks=number_tasks,**kwargs)
            self.flowcell_key = pipeline.flowcell_key
 
    def __push_samples_into_relevant_pipelines__(configs,mockdb):
        """
        Provides the interface from which all post casava pipelines are run.
        """
        if configs["system"].get("Logging","debug") is "True":
            print "  Starting post casava pipelines for " + self.flowcell_key
            print "  Determining Sample dirs"
        sample_dirs = list_sample_dirs(seq_run.output_dir.split(":"))
        if configs["system"].get("Logging","debug") is "True":
           print "  Samples: " + str(sample_dirs) 
        flowcell_dir_name = os.path.basename(self.output_dir)
        automation_parameters_config = ConfigParser.ConfigParser()
        automation_parameters_config.read(configs["system"].get("Filenames","automation_config"))
        for sample in sample_dirs:
            if configs["system"].get("Logging","debug") is "True":
               print "    Processing " + sample
            #running_location = identify_running_location_with_most_currently_available(configs,storage_devices)
            running_location = "Speed"
            mockdb['FastQCPipeline'].__new__(configs['system'],input_dir=sample_dirs[sample][0],pipeline_key=pipeline_key,project=parsed['project_name'],flowcell_dir_name=flowcell_dir_name,**parsed)#Always start the FastQCPipleine!
            parsed = parse_sample_sheet(configs['system'],mockdb,sample_dirs[sample][0])
            description_pieces = parsed['description'].split('-')
            pipeline_key = description_pieces[-1]
            pipeline_name = safe_get(autmation_parameters_config,"Pipeline",pipeline_key)
            if pipeline_name is None:
                continue
            mockdb[pipeline_name].__new__(configs['system'],input_dir=sample_dirs[sample][0],pipeline_key=pipeline_key,project=parsed['project_name'],flowcell_dir_name=flowcell_dir_name,**parsed)
 
    def __is_complete__(self,configs,mockdb,*args,**kwargs):
        """
        Handles the merging of casava directories run in parallel and launching pipelines as well as the normal check.
        """
        if GenericProcess.__is_complete__(self,*args,**kwargs):
            return True
        if not os.path.isfile(self.complete_file):
            return False
        ##Handle merging
        split_categories = []
        if self.split_by_index_length == "True":
            split_categories.append("Index_length")
        if self.split_by_lane == "True":
            split_categories.append("Lane")
        split_dir = os.path.join(self.output_dir,"split")
        merge_split_casava_results(split_dir,self.output_dir,split_categories)
        shutil.rmtree(split_dir)
        ##Launch pipelines
        self.__launch_pipelines__(configs,mockdb)
        QsubProcess.__finish__(*args,**kwargs)
        return True
                    
                
            
       
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
