import datetime
import os
import pytz
import subprocess
import re
import sys
from time import strftime, localtime
from mockdb.models import NumberedObject
from mockdb.scripts import translate_underscores_to_capitals
from physical_objects.models import Sample
from physical_objects.hiseq.models import Flowcell, Barcode
from sge_queries.nodes import grab_good_node
from sge_queries.jobs import check_if_single_job_running_on_system
from template.scripts import fill_template
from processes.transitions import begin_next_step

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

    def __finish__(self,date=strftime("%Y%m%d",localtime()),time=strftime("%H:%M",localtime())):
        """
        Finishes the generic process by changing the state to complete and recording the date and time.
        """
        self.__change_state__('Complete')
        self.end_time = time
        self.end_date = date

    def __is_complete__(self):
        """
        Returns true if self.state is 'Complete'.
        """
        return self.state == 'Complete'

class QsubProcess(GenericProcess):
    """
    Anything process submitted via qsub requires additional information.  This class keeps track of that information
    """

    def __init__(self,config,key=int(-1),input_dir=None,base_output_dir=None,output_dir=None,date=strftime("%Y%m%d",localtime()),time=strftime("%H:%M:%S",localtime()),process_name='qsub',complete_file=None,qsub_file=None,**kwargs):
        """
        Initializes the qsub object.
        """
        GenericProcess.__init__(self,config,key=key,date=date,time=time,process_name=process_name,**kwargs)
        self.input_dir = input_dir
        if output_dir is None:
            if base_output_dir is None:
                base_output_dir = config.get('Common_directories','bcbio_output')
            self.output_dir = base_output_dir
        else:
            self.output_dir = output_dir
        if not os.path.exists(self.output_dir) and not re.search('dummy',self.output_dir):
            os.makedirs(self.output_dir)
        self.stderr = os.path.join(self.output_dir, self.process_name + '.stderr')
        self.stdout = os.path.join(self.output_dir, self.process_name + '.stdout')
        if complete_file is None:
            self.complete_file = os.path.join(self.output_dir, self.process_name + '.complete')
        else:
            self.complete_file = complete_file
        if qsub_file is None:
            self.qsub_file = os.path.join(self.output_dir, self.process_name + '.sh')
        self.job_id = None
        self.fail_reported = False #Hook to provide only a single e-mail when job fails.
    
    def __launch__(self,config,qsub_file=None,node_list=None,queue_name=None):
        """
        Sends the job to SGE and records pertinent information.
        """
        if os.path.isfile(self.complete_file):
            os.remove(self.complete_file)
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

    def __is_complete__(self):
        """
        Checks to see if the complete file is created.
        """
        if GenericProcess.__is_complete__(self) is False:
            return os.path.isfile(self.complete_file)
        else:
            return True

    def __present_on_system__(self):
        """
        Checks to see if the job id is still listed via qstat.  This is not currently working.
        """
        return check_if_single_job_running_on_system(self.job_id)

    def __fill_qsub_file__(self,configs):
        """
        Simply fills process_name template with appropriate info. 
        """
        template_file= os.path.join(configs['system'].get('Common_directories','template'),configs['pipeline'].get('Template_files',self.process_name))
        dictionary = {}
        with open(self.qsub_file,'w') as f:
            f.write(fill_template(template_file,self.__dict__))

class SampleQsubProcess(QsubProcess):
    """
    Adds the sample object to the qsub process.  This has ramifications on the output directory
    """
    def __init__(self,config,key=int(-1),sample=None,input_dir=None,base_output_dir=None,output_dir=None,date=strftime("%Y%m%d",localtime()),time=strftime("%H:%M:%S",localtime()),process_name='sample_qsub',complete_file=None,**kwargs):
        if sample is None:
            sample = Sample(config,key="dummy_sample_key")
        if sample.__class__.__name__ != "Sample":
            raise Exception("Trying to start a qcpipeline process on a non-sample.")
        self.sample_key = sample.key
        if output_dir is None:
            if base_output_dir is None:
                base_output_dir = config.get('Common_directories','bcbio_output')
            self.output_dir = os.path.join(base_output_dir,self.sample_key + '_' + str(date))
        else:
            self.output_dir = output_dir
        QsubProcess.__init__(self,config,key=key,input_dir=input_dir,base_output_dir=base_output_dir,output_dir=self.output_dir,date=date,time=time,process_name=process_name,complete_file=complete_file,**kwargs)


class QualityControlPipeline(GenericProcess):

    def __init__(self,config,key=int(-1),sample=None,barcode=None,description=None,recipe=None,input_dir=None,base_output_dir=None,output_dir_front=None,date=strftime("%Y%m%d",localtime()),time=strftime("%H:%M:%S",localtime()),process_name='qcpipeline',sequencing_run=None,running_location='Speed',storage_needed=None,project=None,**kwargs):
        if sample is None:
            sample = Sample(config,key="dummy_sample_key")
        if sample.__class__.__name__ != "Sample":
            raise Exception("Trying to start a qcpipeline process on a non-sample.")
        if barcode is None:
            barcode = Barcode(config,key="dummy_barcode_key")
        if barcode.__class__.__name__ != "Barcode":
            raise Exception("Trying to start a qcpipeline process on a non-barcode.")
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
        #if base_output_dir == None:
        #    base_output_dir = config.get('Common_directories','bcbio_upload')
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

    def __finish__(self,storage_device,date=datetime.date.today().strftime("%Y%m%d"),time=datetime.date.today().strftime("%H:%M")):
        GenericProcess.__finish__(self,date=date,time=time)
        storage_device.my_use -= self.storage_needed

class StandardPipeline(GenericProcess):

    def __init__(self,config,key=int(-1),sample=None,barcode=None,flowcell=None,description=None,recipe=None,input_dir=None,base_output_dir=None,date=strftime("%Y%m%d",localtime()),time=strftime("%H:%M:%S",localtime()),process_name='qcpipeline',sequencing_run=None,running_location='Speed',storage_needed=500000000,project=None,**kwargs):
        if sample is None:
            sample = Sample(config,key="dummy_sample_key")
        if sample.__class__.__name__ != "Sample":
            raise Exception("Trying to start a qcpipeline process on a non-sample.")
        if barcode is None:
            barcode = Barcode(config,key="dummy_barcode_key")
        if barcode.__class__.__name__ != "Barcode":
            raise Exception("Trying to start a qcpipeline process on a non-barcode.")
        #Specific information about this pipeline
        self.description = description
        self.recipe = recipe
        self.storage_needed = storage_needed
        self.input_dir = input_dir
        self.running_location = running_location
        self.date = date
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
                self.output_dir = project_out + "_" + sample.key + '_' + str(date)
            else:
                self.output_dir = os.path.join(base_output_dir,project_out + "_" + sample.key + '_' + str(date))
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
        self.altered_parameters = None

    def __finish__(self,storage_device,date=datetime.date.today().strftime("%Y%m%d"),time=datetime.date.today().strftime("%H:%M")):
        GenericProcess.__finish__(self,date=date,time=time)
        storage_device.my_use -= int(self.storage_needed or 0)

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
 
    def __handle_linear_steps__(self,configs,storage_devices,mockdb):
        if not self.__check_first_step__(configs["pipeline"]):
            print("The pipeline for "+self.sample_key+" has not started but is apparently running.  Moving to initiated but not running.")
            return 1
        step_order, step_objects = self.__steps_to_objects__(configs["system"],configs["pipeline"],mockdb)
        prev_step = step_order[0]
        for current_step in step_order[1:]:
            if step_objects[current_step] is None: #This means the current step hasn't begun (and is really the next step)
                if step_objects[prev_step].__is_complete__(configs): #Check to see if the previous step has completed.
                    step_objects[prev_step].__finish__()
                    step_objects = begin_next_step(configs,mockdb,self,step_objects,current_step,prev_step)
                return 1
            prev_step = current_step
        if step_objects[prev_step].__is_complete__(configs): #Check to see if the last step has completed.
            step_objects[prev_step].__finish__()
            self.__finish__(storage_device=storage_devices[self.running_location])
        return 1
            
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

class MHCPipeline(StandardPipeline):

    def __init__(self,config,key=int(-1),sample=None,barcode=None,flowcell=None,description=None,recipe=None,input_dir=None,base_output_dir=None,date=strftime("%Y%m%d",localtime()),time=strftime("%H:%M:%S",localtime()),process_name='mhcpipeline',sequencing_run=None,running_location='Space',storage_needed=500000000,project=None,**kwargs):
        if sample is None:
            sample = Sample(config,key="dummy_sample_key")
        if sample.__class__.__name__ != "Sample":
            raise Exception("Trying to start a qcpipeline process on a non-sample.")
        if barcode is None:
            barcode = Barcode(config,key="dummy_barcode_key")
        if barcode.__class__.__name__ != "Barcode":
            raise Exception("Trying to start a qcpipeline process on a non-barcode.")
        #The keys for the sub-processes in this pipeline
        self.zcat_key = None
        self.bcbio_key = None
        self.cleaning_key = None
        #Specific information about this pipeline
        self.description = description
        self.recipe = recipe
        self.storage_needed = storage_needed
        self.input_dir = input_dir
        self.running_location = running_location
        self.date = date
        if project is None:
            if base_output_dir is None:
                base_output_dir = ""
            self.output_dir = os.path.join(base_output_dir,sample.key + '_' + str(date))
        else:
            project_out = re.sub('_','-',project)
            if re.search("[0-9]",project_out[0:1]):
                project_out = "Project-" + project_out
            if base_output_dir == None:
                self.output_dir = project_out + "_" + sample.key + '_' + str(date)
            else:
                self.output_dir = os.path.join(base_output_dir,project_out + "_" + sample.key + '_' + str(date))
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

    def __finish__(self,storage_device,date=datetime.date.today().strftime("%Y%m%d"),time=datetime.date.today().strftime("%H:%M")):
        GenericProcess.__finish__(self,date=date,time=time)
        storage_device.my_use -= self.storage_needed

class RD2Pipeline(StandardPipeline):

    def __init__(self,config,key=int(-1),sample=None,barcode=None,flowcell=None,description=None,recipe=None,input_dir=None,base_output_dir=None,date=strftime("%Y%m%d",localtime()),time=strftime("%H:%M:%S",localtime()),process_name='rd2pipeline',sequencing_run=None,running_location='Space',storage_needed=500000000,project=None,**kwargs):
        if sample is None:
            sample = Sample(config,key="dummy_sample_key")
        if sample.__class__.__name__ != "Sample":
            raise Exception("Trying to start a qcpipeline process on a non-sample.")
        if barcode is None:
            barcode = Barcode(config,key="dummy_barcode_key")
        if barcode.__class__.__name__ != "Barcode":
            raise Exception("Trying to start a qcpipeline process on a non-barcode.")
        #The keys for the sub-processes in this pipeline
        self.zcat_key = None
        self.bcbio_key = None
        self.cleaning_key = None
        #Specific information about this pipeline
        self.description = description
        self.recipe = recipe
        self.storage_needed = storage_needed
        self.input_dir = input_dir
        self.running_location = running_location
        self.date = date
        if project is None:
            if base_output_dir is None:
                base_output_dir = ""
            self.output_dir = os.path.join(base_output_dir,sample.key + '_' + str(date))
        else:
            project_out = re.sub('_','-',project)
            if re.search("[0-9]",project_out[0:1]):
                project_out = "Project-" + project_out
            if base_output_dir == None:
                self.output_dir = project_out + "_" + sample.key + '_' + str(date)
            else:
                self.output_dir = os.path.join(base_output_dir,project_out + "_" + sample.key + '_' + str(date))
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

    def __finish__(self,storage_device,date=datetime.date.today().strftime("%Y%m%d"),time=datetime.date.today().strftime("%H:%M")):
        GenericProcess.__finish__(self,date=date,time=time)
        storage_device.my_use -= self.storage_needed

class DevelPipeline(StandardPipeline):

    def __init__(self,config,key=int(-1),sample=None,barcode=None,flowcell=None,description=None,recipe=None,input_dir=None,base_output_dir=None,date=strftime("%Y%m%d",localtime()),time=strftime("%H:%M:%S",localtime()),process_name='develpipeline',sequencing_run=None,running_location='Speed',storage_needed=500000000,project=None,**kwargs):
        if sample is None:
            sample = Sample(config,key="dummy_sample_key")
        if sample.__class__.__name__ != "Sample":
            raise Exception("Trying to start a qcpipeline process on a non-sample.")
        if barcode is None:
            barcode = Barcode(config,key="dummy_barcode_key")
        if barcode.__class__.__name__ != "Barcode":
            raise Exception("Trying to start a qcpipeline process on a non-barcode.")
        #The keys for the sub-processes in this pipeline
        self.zcat_key = None
        self.bcbio_key = None
        self.cleaning_key = None
        #Specific information about this pipeline
        self.description = description
        self.recipe = recipe
        self.storage_needed = storage_needed
        self.input_dir = input_dir
        self.running_location = running_location
        self.date = date
        if project is None:
            if base_output_dir is None:
                base_output_dir = ""
            self.output_dir = os.path.join(base_output_dir,sample.key + '_' + str(date))
        else:
            project_out = re.sub('_','-',project)
            if re.search("[0-9]",project_out[0:1]):
                project_out = "Project-" + project_out
            if base_output_dir == None:
                self.output_dir = project_out + "_" + sample.key + '_' + str(date)
            else:
                self.output_dir = os.path.join(base_output_dir,project_out + "_" + sample.key + '_' + str(date))
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

    def __finish__(self,storage_device,date=datetime.date.today().strftime("%Y%m%d"),time=datetime.date.today().strftime("%H:%M")):
        GenericProcess.__finish__(self,date=date,time=time)
        storage_device.my_use -= self.storage_needed

class BBPipeline(StandardPipeline):

    def __init__(self,config,key=int(-1),sample=None,barcode=None,flowcell=None,description=None,recipe=None,input_dir=None,base_output_dir=None,date=strftime("%Y%m%d",localtime()),time=strftime("%H:%M:%S",localtime()),process_name='bbpipeline',sequencing_run=None,running_location='Speed',storage_needed=500000000,project=None,**kwargs):
        if sample is None:
            sample = Sample(config,key="dummy_sample_key")
        if sample.__class__.__name__ != "Sample":
            raise Exception("Trying to start a qcpipeline process on a non-sample.")
        if barcode is None:
            barcode = Barcode(config,key="dummy_barcode_key")
        if barcode.__class__.__name__ != "Barcode":
            raise Exception("Trying to start a qcpipeline process on a non-barcode.")
        #The keys for the sub-processes in this pipeline
        self.zcat_key = None
        self.bcbio_key = None
        self.cleaning_key = None
        #Specific information about this pipeline
        self.description = description
        self.recipe = recipe
        self.storage_needed = storage_needed
        self.input_dir = input_dir
        self.running_location = running_location
        self.date = date
        if project is None:
            if base_output_dir is None:
                base_output_dir = ""
            self.output_dir = os.path.join(base_output_dir,sample.key + '_' + str(date))
        else:
            project_out = re.sub('_','-',project)
            if re.search("[0-9]",project_out[0:1]):
                project_out = "Project-" + project_out
            if base_output_dir == None:
                self.output_dir = project_out + "_" + sample.key + '_' + str(date)
            else:
                self.output_dir = os.path.join(base_output_dir,project_out + "_" + sample.key + '_' + str(date))
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

    def __finish__(self,storage_device,date=datetime.date.today().strftime("%Y%m%d"),time=datetime.date.today().strftime("%H:%M")):
        GenericProcess.__finish__(self,date=date,time=time)
        storage_device.my_use -= self.storage_needed

class KanePipeline(StandardPipeline):

    def __init__(self,config,key=int(-1),sample=None,barcode=None,flowcell=None,description=None,recipe=None,input_dir=None,base_output_dir=None,date=strftime("%Y%m%d",localtime()),time=strftime("%H:%M:%S",localtime()),process_name='kanepipeline',sequencing_run=None,running_location='Speed',storage_needed=500000000,project=None,**kwargs):
        if sample is None:
            sample = Sample(config,key="dummy_sample_key")
        if sample.__class__.__name__ != "Sample":
            raise Exception("Trying to start a qcpipeline process on a non-sample.")
        if barcode is None:
            barcode = Barcode(config,key="dummy_barcode_key")
        if barcode.__class__.__name__ != "Barcode":
            raise Exception("Trying to start a qcpipeline process on a non-barcode.")
        #The keys for the sub-processes in this pipeline
        self.zcat_key = None
        self.bcbio_key = None
        self.cleaning_key = None
        #Specific information about this pipeline
        self.description = description
        self.recipe = recipe
        self.storage_needed = storage_needed
        self.input_dir = input_dir
        self.running_location = running_location
        self.date = date
        if project is None:
            if base_output_dir is None:
                base_output_dir = ""
            self.output_dir = os.path.join(base_output_dir,sample.key + '_' + str(date))
        else:
            project_out = re.sub('_','-',project)
            if re.search("[0-9]",project_out[0:1]):
                project_out = "Project-" + project_out
            if base_output_dir == None:
                self.output_dir = project_out + "_" + sample.key + '_' + str(date)
            else:
                self.output_dir = os.path.join(base_output_dir,project_out + "_" + sample.key + '_' + str(date))
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

    def __finish__(self,storage_device,date=datetime.date.today().strftime("%Y%m%d"),time=datetime.date.today().strftime("%H:%M")):
        GenericProcess.__finish__(self,date=date,time=time)
        storage_device.my_use -= self.storage_needed

class NGv3PlusPipeline(StandardPipeline):

    def __init__(self,config,key=int(-1),sample=None,barcode=None,flowcell=None,description=None,recipe=None,input_dir=None,base_output_dir=None,date=strftime("%Y%m%d",localtime()),time=strftime("%H:%M:%S",localtime()),process_name='ngv3pluspipeline',sequencing_run=None,running_location='Speed',storage_needed=500000000,project=None,**kwargs):
        StandardPipeline.__init__(self,config,key,sample,barcode,flowcell,description,recipe,input_dir,base_output_dir,date,time,process_name,sequencing_run,running_location,storage_needed,project,**kwargs)
        self.summary_stats_key = None
        self.cp_to_results_key = None
