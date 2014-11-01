import datetime
import shutil
import os
import pytz
import subprocess
import re
import sys
from config.scripts import MyConfigParser
from manage_storage.disk_queries import disk_usage
from time import strftime, localtime
from mockdb.models import NumberedObject
from mockdb.scripts import translate_underscores_to_capitals
from physical_objects.models import Sample
from physical_objects.hiseq.models import Flowcell, Barcode
from sge_email.scripts import send_email
from sge_queries.nodes import grab_good_node
from sge_queries.jobs import check_if_single_job_running_on_system
from template.scripts import fill_template
from processes.transitions import begin_next_step
from processes.hiseq.scripts import list_sample_dirs, copy_all_xml

class GenericProcess(NumberedObject):
    """
    A generic class for any process.
    """

    def __init__(self,config,key=int(-1),date=strftime("%Y%m%d",localtime()),time=strftime("%H:%M:%S",localtime()),process_name='generic',**kwargs):
        """
        Initializes the generic process object.
        """
        NumberedObject.__init__(self,config,key=key,**kwargs)
        self.date_begin = date
        self.time_begin = time
        self.end_time = None
        self.end_date = None
        self.state = 'Initialized'
        self.process_name = process_name

    def __is_running__(self):
        """
        Checks the state of the process.  Returns true if state is identically 'Running'.
        """
        return self.state == 'Running'
    
    def __change_state__(self,new_state):
        """
        A wrapper for changing the state.  This provides a hook for later adding checks and what not when states are changed.
        """
        self.state = new_state

    def __finish__(self,*args,**kwargs):
        """
        Finishes the generic process by changing the state to complete and recording the date and time.
        """
        self.__change_state__('Complete')
        self.end_time = strftime("%H:%M",localtime())
        self.end_date = strftime("%Y%m%d",localtime())

    def __is_complete__(self,*args,**kwargs):
        """
        Returns true if self.state is 'Complete'.
        """
        try:
            return self.state == 'Complete'
        except AttributeError:
            #sys.stderr.write("The proess has no attribute state for sample "+self.sample_key)
            return False

class QsubProcess(GenericProcess):
    """
    Any process submitted via qsub requires additional information.  This class keeps track of that information.  I recently added the number_tasks
    attribute to handle task parallelization.
    """

    def __init__(self,config,key=int(-1),input_dir=None,base_output_dir=None,output_dir=None,process_name='qsub_task',number_tasks=1,complete_file=None,**kwargs):
        """
        Initializes the qsub object.
        """
        GenericProcess.__init__(self,config,key=key,process_name=process_name,**kwargs)
        self.input_dir = input_dir
        if output_dir is None:
            if base_output_dir is None:
                base_output_dir = config.get('Common_directories','base_working_directory')
            self.output_dir = base_output_dir
        else:
            self.output_dir = output_dir
        if not os.path.exists(self.output_dir) and not re.search('dummy',self.output_dir):
            os.makedirs(self.output_dir)
        self.qsub_file = os.path.join(self.output_dir, self.process_name + '.sh')
        self.stderr = os.path.join(self.output_dir, self.process_name + '.stderr')
        self.stdout = os.path.join(self.output_dir, self.process_name + '.stdout')
        self.number_tasks = number_tasks
        if complete_file is None:
            complete_files = []
            tmp_dirs = []
            for i in range(self.number_tasks):
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
            self.complete_file = complete_file
        self.job_id = None
        self.fail_report = False

    def __launch__(self,config,command=None,qsub_file=None,node_list=None,queue_name=None):
        """
        Sends the job to SGE and records pertinent information.
        """
        if os.path.isfile(self.complete_file):
            os.remove(self.complete_file)
        if command is None:
            command = ['qsub']
        if not node_list is None:
            try:
                node = grab_good_node(config,node_list=node_list)
                command.extend(['-l','hostname='+node])
            except Exception, msg:
                sys.stderr.write("{0}\n".format(msg))
                return False
        if qsub_file is None:
            qsub_file = self.qsub_file
        if not queue_name is None:
            qname = 'qname=' + queue_name
            command.extend(['-l',qname])
        command.append(qsub_file)
        activate_qsub = "source /opt/sge625/sge/default/common/settings.sh"
        sys.stderr.write("Submitting "+self.qsub_file+"\n")
        proc = subprocess.Popen(activate_qsub + ";"+" ".join(command),shell=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        out = proc.stdout.read()
        submission_file = re.sub(self.output_dir + "/","",self.qsub_file)
        expectation = "Your job ([0-9]+) \(\"" + submission_file  + "\"\) has been submitted"
        self.state = 'Running'
        self.fail_reported = False
        match = re.search(expectation, out)
        if match:
            self.jobid = match.group(1)
            return True
        return False

    def __is_complete__(self,*args,**kwargs):
        """
        Checks to see if the complete files are created and finishes the object if it is.
        """
        try:
            if GenericProcess.__is_complete__(self) is False:
                complete_files = self.complete_file.split(":")
                for complete_file in complete_files:
                    print complete_file
                    if os.path.isfile(complete_file):
                        continue
                    return False
            self.__finish__(*args,**kwargs)
            return True
        except AttributeError:
            return False

    def __present_on_system__(self):
        """
        Checks to see if the job id is still listed via qstat.  This is not currently working.
        """
        return check_if_single_job_running_on_system(self.job_id)

    def __fill_qsub_file__(self,configs,template_config=None):
        """
        Simply fills process_name template with appropriate info. 
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
            if configs["system"].get("Logging","debug") is "True":
                print "Filling qsub with " + str(self.__dict__)
            f.write(fill_template(template_file,self.__dict__))

    def __finish__(self,*args,**kwargs):
        """
        Finishes the qsub process by also removing the tmp dir(s).
        """
        GenericProcess.__finish__(self,*args,**kwargs)
        if hasattr(self,'tmp_dir') and not self.tmp_dir is None:
            for tmp_dir in self.tmp_dir.split(":"):
                if os.path.isdir(tmp_dir):
                    sys.stderr.write("  Attempting to remove " + tmp_dir + "\n")
                    if not re.search('template',tmp_dir):
                        shutil.rmtree(tmp_dir)
        for complete_file in self.complete_file.split(":"):
            if not os.path.isfile(complete_file):
                with open(complete_file,'a'):
                    os.utime(complete_file, None)
            

class SampleQsubProcess(QsubProcess):
    """
    Adds the sample object to the qsub process.  This has ramifications on the output directory
    """
    def __init__(self,config,key=int(-1),sample=None,process_name='sample_qsub',output_dir=None,base_output_dir=None,date=strftime("%Y%m%d",localtime()),**kwargs):
        if sample is None:
            sample = Sample(config,key="dummy_sample_key")
        if sample.__class__.__name__ != "Sample":
            raise Exception("Trying to start a qcpipeline process on a non-sample.")
        self.sample_key = sample.key
        if output_dir is None:
            if base_output_dir is None:
                base_output_dir = config.get('Common_directories','base_working_directory')
            self.output_dir = os.path.join(base_output_dir,self.sample_key + '_' + str(date))
        else:
            self.output_dir = output_dir
        QsubProcess.__init__(self,config,key=key,output_dir=self.output_dir,process_name=process_name,**kwargs)

class Bam2BamQsubProcess(SampleQsubProcess):
    """
    A number of jobs take a bam as an input and output the bam.  This generalizes the initialization of such
    jobs.  New bam description is the string that will be inserted in front of .bam.  If None, the input bam 
    is the same as the output (overwritten, I'd imagine). 
    """
    def __init__(self,config,key=int(-1),prev_step=None,new_bam_description=None,output_bam=None,process_name='bam2bam_qsub',**kwargs):
        SampleQsubProcess.__init__(self,config,key=key,process_name=process_name,**kwargs)
        if not prev_step is None:
            self.input_bam = prev_step.output_bam
            if not new_bam_description is None:
                self.output_bam, number = re.subn(r".bam","."+new_bam_description+".bam",self.input_bam)
            else:
                if output_bam is None:
                    self.output_bam = self.input_bam
                else:
                    self.output_bam = output_bam

    def __is_complete__(self,*args,**kwargs):
        if QsubProcess.__is_complete__(self,*args,**kwargs):
            for output_bam in self.output_bam.split(":"):
                if not os.path.isfile(output_bam):
                    return False
            return True
        return False

class QualityControlPipeline(GenericProcess): #I am not positive this still works.

    def __init__(self,config,key=int(-1),sample=None,barcode=None,description=None,recipe=None,input_dir=None,base_output_dir=None,output_dir_front=None,date=strftime("%Y%m%d",localtime()),time=strftime("%H:%M:%S",localtime()),process_name='qcpipeline',sequencing_run=None,running_location='Speed',storage_needed=None,project=None,**kwargs):
        if not sample is None:
            #The keys for the sub-processes in this pipeline
            self.zcat_key = None
            self.bcbio_key = None
            self.snp_stats_key = None
            self.cleaning_key = None
            #Specific information about this pipeline
            self.description = description
            self.recipe = recipe
            #if storage_needed is None:
            #    self.storage_needed = config.get('Storage','needed')
            #else:
            self.storage_needed = storage_needed
            self.input_dir = input_dir
            self.running_location = running_location
            self.date = date
            self.project = project
            if project is None:
                if not base_output_dir is None:
                    self.output_dir = os.path.join(base_output_dir,sample.key + '_' + str(date))
                else:
                    self.output_dir = os.path.join(sample.key + '_' + str(date))
            else:
                project_out = re.sub('_','-',project)
                if re.search("[0-9]",project_out[0:1]):
                    project_out = "Project-" + project_out
                if not base_output_dir is None:
                    self.output_dir = os.path.join(base_output_dir,project_out + "_" + sample.key + '_' + str(date))
                else:
                    self.output_dir = project_out + "_" + sample.key + '_' + str(date)
            if not os.path.exists(self.output_dir) and not re.search('dummy',sample.key):
                os.makedirs(self.output_dir)
            if sequencing_run != None:
                self.sequencing_run_key=seqencing_run.key
            else:
                self.sequencing_key=None
            GenericProcess.__init__(self,config,key=key,process_name=process_name,**kwargs)
            self.sample_key = sample.key
            self.flowcell_key = barcode.flowcell_key
            self.barcode_key = barcode.key

    def __finish__(self,storage_device,*args,**kwargs):
        GenericProcess.__finish__(self,*args,**kwargs)
        storage_device.my_use -= self.storage_needed

class GenericPipeline(GenericProcess):
    """
    Generalization of any series of processes that follow one another serially.
    """

    def __init__(self,config,key=-1,process_name="generic_pipeline",**kwargs):
        GenericProcess.__init__(self,config,key=key,process_name=process_name,**kwargs)

    def __get_step_key__(self,step):
       """Returns the pipeline key corresponding to a specific step"""
       try:
           return getattr(self,step + "_key")
       except:
           if step == "clean_bcbio":
              try:
                  return getattr(self,"cleaning_key")
              except:
                  return None
           return None

    def __get_step_obj__(self,system_config,mockdb,step):
        """Returns the database object corresponding to the given step and key."""
        step_key = self.__get_step_key__(step)
        if step_key == None:
            return None
        return mockdb[translate_underscores_to_capitals(step)].__get__(system_config,int(getattr(self,step+"_key")))

    
    def __steps_to_objects__(self,system_config,pipeline_config,mockdb):
        """  Translates the steps of the pipeline to the appropriate object"""
        steps = pipeline_config.get("Pipeline","steps").split(",")
        step_objects = {}
        for step in steps:
            step_objects[step] = self.__get_step_obj__(system_config,mockdb,step)
        return (steps, step_objects)

    def __check_first_step__(self,pipeline_config):
        steps = pipeline_config.get("Pipeline","steps").split(",")
        step_key = self.__get_step_key__(steps[0])
        if step_key is None:
            self.state = 'Initialized'
            return False
        return True
 
    def __handle_linear_steps__(self,configs,mockdb,storage_devices=None,skip_finish=False,*args,**kwargs):
        if not self.__check_first_step__(configs["pipeline"]):
            print("The pipeline for "+self.sample_key+" has not started but is apparently running.  Moving to initiated but not running.")
            return 1
        step_order, step_objects = self.__steps_to_objects__(configs["system"],configs["pipeline"],mockdb)
        prev_step_key = step_order[0]
        for current_step_key in step_order[1:]:
            if configs["system"].get("Logging","debug") is "True":
                print current_step_key
            if step_objects[current_step_key] is None: #This means the variable name in "current step" hasn't begun (and is really the next step)
                if configs["system"].get("Logging","debug") is "True":
                    print "  Checking to see if this process should begin" 
                    print "    by checking if "+prev_step_key+" is complete" 
                if step_objects[prev_step_key].__is_complete__(configs,mockdb,*args,**kwargs):
                    if configs["system"].get("Logging","debug") is "True":
                       print "  It should" 
                    step_objects[prev_step_key].__finish__(*args,**kwargs)
                    step_objects = begin_next_step(configs,mockdb,self,step_objects,current_step_key,prev_step_key)
                return False
            prev_step_key = current_step_key
        if step_objects[prev_step_key].__is_complete__(configs,mockdb,*args,**kwargs): #Check to see if the last step has completed.
            step_objects[prev_step_key].__finish__(configs,*args,**kwargs)
            if not skip_finish:
                if not storage_devices is None:
                    self.__finish__(storage_device=storage_devices[self.running_location],*args,**kwargs)
                else:
                    self.__finish__(*args,**kwargs)
            return True
        return False
            
    def __copy_altered_parameters_to_config__(self,config):
        altered_parameters = self.__expand_altered_parameters__()
        for section in altered_parameters:
            for option in altered_parameters[section]:
                config.set(section,option,altered_parameters[section][option])
        return 1

    def  __alter_parameter__(self,section,option,value):
        """
        Provides an interface to store a parameter that you want different for the specific pipeline
        compared to what is specified in the pipeline_config.
        """
        altered_parameters = self.__expand_altered_parameters__()
        altered_parameters[section].update({option: value})
        self.__flatten_2D_dictionary__(altered_parameters,"altered_parameters")
        return True
        

    def  __expand_altered_parameters__(self):
        """
        Converts the flat altered_parameters attribute, section-name:value to a dictionary
        dict[section][name]=value.
        """
        expanded = {}
        if not hasattr(self,"altered_parameters") or self.altered_parameters is None:
            return expanded
        parameters_info = self.altered_parameters.split(";")
        for parameter_info in parameters_info:
            info, value = parameter_info.split(":")
            section, option = info.spit("-")
            if not section in expanded:
                expanded[section] = {}
            expanded[section].update({option: value})
        return expanded

    def  __flatten_2D_dictionary__(self,dictionary,attribute):
        """
        Flattens and stores the dictionary in key1-key2:value format
        in the pipeline's attribute.
        """
        output_string = ""
        for key1 in dictionary:
            for key2 in dictionary[key1]:
                flattened_string = key1 + "-" + key2 + ":" + dictionary[key1][key2]
                output_string += flattened_string
        return output_string

    def __finish__(self,storage_device=None,*args,**kwargs):
        GenericProcess.__finish__(self,*args,**kwargs)
        if not storage_device is None:
            storage_device.my_use -= int(self.storage_needed or 0)

class StandardPipeline(GenericPipeline):
    """
    Generalization of any post fastq pipeline that requires a sample and a sample sheet.
    """

    def __init__(self,config,key=int(-1),sample=None,description=None,recipe=None,input_dir=None,pipeline_config=None,pipeline_key=None,process_name='qcpipeline',running_location='Speed',storage_needed=500000000,project=None,flowcell_dir_name=None,seq_run_key=None,*args,**kwargs):
        if not pipeline_config is None or not pipeline_key is None:
            if sample is None:
                sample = Sample(config,key="dummy_sample_key")
            if sample.__class__.__name__ != "Sample":
                raise Exception("Trying to start a qcpipeline process on a non-sample.")
            automation_parameters_config = ConfigParser.ConfigParser()
            automation_parameters_config.read(configs["system"].get("Filenames","automation_config"))
            #Specific information about this pipeline
            self.description = description
            self.recipe = recipe
            self.storage_needed = storage_needed
            self.input_dir = input_dir
            self.running_location = running_location
            self.seq_run_key = seq_run_key
            capture_target_bed = safe_get(autmation_parameters_config,"Target",pipeline_key)
            if not capture_target_bed is None:
                self.capture_target_bed = capture_target_bed
            if pipeline_config is None:
                pipeline_name = safe_get(autmation_parameters_config,"Pipeline",pipeline_key)
                pipeline_config = ConfigParser.ConfigParser()
                pipeline_config.read(system_config.get('Pipeline',pipeline_name))
            pipeline_steps = pipeline_config.get('Pipeline','steps')
            for step in pipeline_steps:
                setattr(self,step+"_key",None)
            base_output_dir = pipeline_config.get('Common_directories','archive_directory')
            if flowcell_dir_name is None:
                self.client_dir = self.input_dir
            else:
                sample_dir_name = sample.key
                if not str(sample_dir_name).beginswith("Sample_"):
                    sample_dir_name = "Sample_" + sample_dir_name
                self.client_dir = os.path.join(config.get('Common_directories','casava_output'),flowcell_dir_name+"/Project_"+str(project)+"/"+sample_dir_name)
            base_client_dir = config.get('Common_directories','casava_output')
            if project is None:
                if base_output_dir is None:
                    base_output_dir = ""
                self.output_dir = os.path.join(base_output_dir,sample.key + '_' + str(date))
            else:
                project_out = re.sub('_','-',project)
                self.project = project_out
                if re.search("[0-9]",project_out[0:1]):
                    project_out = "Project-" + project_out
                if base_output_dir == None:
                    base_output_dir = ""
                self.output_dir = os.path.join(base_output_dir,project_out + "_" + sample.key + '_' + str(date))
            if not os.path.exists(self.output_dir) and not re.search('dummy',sample.key):
                os.makedirs(self.output_dir)
            GenericProcess.__init__(self,config,key=key,process_name=process_name,**kwargs)
            self.date = date
            self.sample_key = sample.key
            self.altered_parameters = None

class MHCPipeline(StandardPipeline):

    def __init__(self,config,key=int(-1),process_name='mhcpipeline',*args,**kwargs):
        StandardPipeline.__init__(self,config,key=key,process_name=process_name,*args,**kwargs)

class RD2Pipeline(StandardPipeline):

    def __init__(self,config,key=int(-1),process_name='rd2pipeline',*args,**kwargs):
        StandardPipeline.__init__(self,config,key=key,process_name=process_name,*args,**kwargs)


class DevelPipeline(StandardPipeline):

    def __init__(self,config,key=int(-1),process_name='develpipeline',*args,**kwargs):
        StandardPipeline.__init__(self,config,key=key,process_name=process_name,*args,**kwargs)

class BBPipeline(StandardPipeline):

    def __init__(self,config,key=int(-1),process_name='bbpipeline',*args,**kwargs):
        StandardPipeline.__init__(self,config,key=key,process_name=process_name,*args,**kwargs)

class KanePipeline(StandardPipeline):

    def __init__(self,config,key=int(-1),process_name='kanepipeline',*args,**kwargs):
        StandardPipeline.__init__(self,config,key=key,process_name=process_name,*args,**kwargs)


class NGv3PlusPipeline(StandardPipeline):

    def __init__(self,config,key=int(-1),process_name='ngv3pluspipeline',*args,**kwargs):
        StandardPipeline.__init__(self,config,key=key,process_name=process_name,*args,**kwargs)

class TCSPipeline(StandardPipeline):

    def __init__(self,config,key=int(-1),process_name='tcspipeline',*args,**kwargs):
        StandardPipeline.__init__(self,config,key=key,process_name=process_name,*args,**kwargs)

class FastQCPipeline(StandardPipeline):

    def __init__(self,config,process_name='fastqcpipeline',**kwargs):
        StandardPipeline.__init__(self,config,process_name=process_name,**kwargs)

class BclToFastqPipeline(GenericPipeline):
    """
    This pipeline calls casava and other scripts to analyze bcl outputs from the HiSeq
    data directroy producing fastq files.  Additional pipelines are launched once the
    fastq files are generated.  Also, additional scripts are launched after fastq generation
    to facilitate the gathering of run statistics.
    """

    def __init__(self,config,key=-1,seq_run=None,running_location='Speed',process_name="bcltofastqpipeline",**kwargs):
        if not seq_run is None:
            self.seq_run_key = seq_run.key
            self.flowcell_key = seq_run.flowcell_key
            self.input_dir = seq_run.output_dir
            output_name = str(seq_run.date) + "_" + str(seq_run.machine_key) + "_" + str(seq_run.run_number) + "_" + str(seq_run.side) + str(seq_run.flowcell_key)
            self.output_dir = os.path.join(config.get('Common_directories','base_working_directory'),process_name+"/"+output_name)
            GenericPipeline.__init__(self,config,key=key,process_name=process_name,**kwargs)
            if not os.path.exists(self.output_dir):
               os.makedirs(self.output_dir)
            self.running_location = running_location

    def __is_complete__(self,configs,mockdb,*args,**kwargs):
        """
        Checks to see if the pipeline is complete.  If not, and it is ready to advance, then
        the pipeline is advanced.
        """
        if GenericProcess.__is_complete__(self,*args,**kwargs):
            return True
        if not hasattr(self,"generic_copy_key") or self.generic_copy_key is None:
            if configs["system"].get("Logging","debug") is "True":
                print "Copying bcls"
            self.__launch_copy_bcls__(configs,mockdb)
            return False
        current_configs = {}
        current_configs["system"] = configs["system"]
        pipeline_config = MyConfigParser()
        current_configs["pipeline"] = pipeline_config
        pipeline_config.read(configs["system"].get('Pipeline','BclToFastqPipeline'))
        if self.__handle_linear_steps__(current_configs,mockdb,skip_finish=True,*args,**kwargs):
            casava = mockdb['Casava'].__get__(configs['system'],self.casava_key)
            if casava.__do_all_relevant_pipelines_have_first_step_complete__(current_configs,mockdb):
                self.__finish__(*args,**kwargs)
                return True
        return False

    def __launch_copy_bcls__(self,configs,mockdb):
        """
        This launches the process that will archive the fastq directories.
        """
        input_dir = os.path.join(self.input_dir,"Data/Intensities")
        output_dir = os.path.join(self.output_dir,"Data/Intensities")
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        copy_all_xml(self.input_dir,self.output_dir)
        copy_all_xml(os.path.join(self.input_dir,"Data"),os.path.join(self.output_dir,"Data"))
        current_configs = {}
        current_configs["system"] = configs["system"]
        pipeline_config = MyConfigParser()
        current_configs["pipeline"] = pipeline_config
        pipeline_config.read(configs["system"].get('Pipeline','BclToFastqPipeline'))
        copy_bcls = mockdb['GenericCopy'].__new__(configs['system'],input_dir=input_dir,output_dir=output_dir)
        self.generic_copy_key = copy_bcls.key
        copy_bcls.__fill_qsub_file__(current_configs)
        copy_bcls.__launch__(configs['system'])

    def __finish__(self,*args,**kwargs):
        """
        Finishes the bcltofastq pipeline.  This is separated
        out due to the consolidation of multiple directories into a single email
        and to isolate it for specific pipelines.
        """
        problem_dirs = []
        sample_dirs = list_sample_dirs(self.output_dir.split(":"))
        for sample in sample_dirs:
            for sample_dir in sample_dirs[sample]:
                if (int(disk_usage(sample_dir)) < 200000):
                    problem_dirs.append(sample_dir)
        if len(problem_dirs) > 0:
            message = "The following directory(ies) is(are) less than 200MB:\n"
            for problem_dir in problem_dirs:
                message += "\t" + problem_dir + "\n"
            message += "Please check.\n"
            #send_email("Small sample directory",message,recipients='zerbeb@humgen.ucsf.edu,LaoR@humgen.ucsf.edu')  
        GenericPipeline.__finish__(self,*args,**kwargs)
        return 1


class DnanexusuploadPipeline(GenericPipeline):

    def __init__(self,config,key=-1,input_dir=None,run_qc_metrics_dir=None,flowcell_key=None,process_name='dnanexusuploadpipeline',**kwargs):
        if not input_dir is None:
            GenericPipeline.__init__(self,config,key=key,input_dir=input_dir,output_dir=input_dir,process_name=process_name,**kwargs)
            self.flowcell_key = flowcell_key

