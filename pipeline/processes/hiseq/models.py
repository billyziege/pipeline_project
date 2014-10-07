import os
import csv
import shutil
from time import strftime, localtime
from physical_objects.hiseq.models import Flowcell, HiSeqMachine
from processes.models import GenericProcess, StandardPipeline
from process.hiseq.sample_sheet import clean_sample_name, copy_sample_sheet, SampleSheetObjList
from process.hiseq.sample_sheet import SampleSheetFormattingError, send_missing_sample_sheet_email
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
        if output_dir == None:
            self.output_dir = os.path.join(config.get('Common_directories','hiseq_output'),output_name)
        else:
            self.output_dir = output_dir
        if complete_file == None:
            self.complete_file = os.path.join(config.get('Common_directories','hiseq_output'),config.get('Filenames','bcls_ready'))
        else:
            self.complete_file = complete_file
        self.no_delete = no_delete

    def __is_complete__(self):
        """
        Checks the complete file and handles any notifications.
        """
        return os.path.isfile(self.complete_file):

    def __start_bcltofastq_pipeline__(self,configs,mockdb):
        bcl2fastq_pipeline = mockdb['BclToFastqPipeline'].__new__(config['system'],input_dir=input_dir,output_dir=output_dir)

    def __archive_sequencing_run_data__(input_dir,output_dir):
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        try:
            sub_dirs = ["InterOp"]
            for sub_dir in sub_dirs:
                shutil.copytree(os.path.join(input_dir,sub_dir),output_dir)
            files = ["First_Base_Report.htm","RunInfo.xml","runParameters.xml"]
            for file in files:
                shutil.copy(os.path.join(input_dir,file),output_dir)
              
class Casava(QsubProcess):
    """
    Keeps track of the casava process and does any needed processing.
    """

    def __init__(self,config,key=-1,prev_step=None,pipeline=None,split_by_lane="True",split_by_index_length="True",**kwargs):
        """
        In addition to initializing, other steps are completed.  These are commented below.
        """
        if prev_step != None:
            input_dir = prev_step.output_dir
            output_dir = os.path.join(prev_step.output_dir,"casava")
            original_sample_sheet_file = os.path.join(pipeline.input_dir,"SampleSheet.csv")
            if not os.path.isfile(original_sample_sheet_file):#Check to make sure original sample sheet exists
               send_missing_sample_sheet_email(original_sample_sheet_file)
               raise SampleSheetFormatException("No sample sheet found: "+str(original_sample_sheet_file))
            sample_sheet_obj_list = SampleSheetObjList(sample_sheet_file=original_sample_sheet_file)
            sample_sheet_obj_list[0].sample_sheet_table.__write_file__(os.path.join(pipeline.output_dir,"SampleSheet.csv"))#Copy sample sheet to final output dir.
            self.split_by_lane = split_by_lane
            if split_by_lane == "True": #Split by lane (speed up especially for high throughput)
                sample_sheet_obj_list = sample_sheet_obj_list.__partition_sample_sheet_objects__(self,"Lane")
            self.split_by_index_length = split_by_index_legnth
            if split_by_index_length == "True": #Split by index lane (prevents casava from breaking when pool samples have different index lengths)
                sample_sheet_obj_list = sample_sheet_obj_list.__partition_sample_sheet_objects__(self,"Index",use_length=True)
            number_tasks = len(sample_sheet_obj_list)
            temporary_output_directories = sample_sheet_obj_list.__create_meta_directories_and_write_files__(os.path.join(output_dir,"split"))
            self.temporary_output_directory = ":".join(temporary_output_directories)
            QsubProcess.__init__(self,config,key=key,number_tasks=number_tasks,**kwargs)
        
class MergeCasava(QsubProcess):
    """
    Handles the merging of casava directories run in parallel in the previous step.
    """
    def __init__(self,config,key=-1,prev_step=None,pipeline=None,split_by_lane="True",split_by_index_length="True",**kwargs):
        """
        In addition to initializing, other steps are completed.  These are commented below.
        """
        if prev_step != None:
            sample_sheet_obj_list = SampleSheetObjList()
            sample_sheet_obj_list.__load_sample_sheets_from_meta_directories__(os.path.join(prev_step.output_dir,"split"),["Index_length","Lane"])
            #Idenfity the min index_length for each lane.
            min_index_lengths = {} #Lane -> index_length
            project_dirs = {}
            #Base the merge off the sample sheet obj list
            for sample_sheet_obj in sample_sheet_obj_list.list:
                lane = sample_sheet_obj.meta_data["Lane"]
                index_length = int(sample_sheet_obj.meta_data["Index_length"])
                split_dir = os.path.join(prev_step.output_dir,"split/"+str(index_length)+"_"+str(lane))
                #Load all directories by Project and Sample directories retaining casava structure
                fastq_dirs = grab_fastq_dirs_keyed_by_project_and_sample(split_dir)
                #Load the undetermined indices only from the analysis with the min index length from each lane
                if not lane in min_index_lengths:
                    min_index_lengths[lane] = index_length
                else:
                    if min_index_length[lane] > index_length:
                        min_index_length[lane] = index_length
            undetermined_dirs = []
            for lane in min_index_length:
                undetermined_dir = os.path.join(prev_step.output_dir,"split/"+str(min_index_length[lane])+"_"+str(lane)+"/Undetermined_indices/Sample_lane"+str(lane))
                undetermined_dirs.append(undetermined_dir)
                    
                
            
       
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
