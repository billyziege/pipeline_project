import os
import re
import sys
from manage_storage.disk_queries import disk_usage
from physical_objects.hiseq.models import Sample
from processes.models import GenericProcess, SampleQsubProcess, Bam2BamQsubProcess
from template.scripts import fill_template
from sge_email.scripts import send_email
from processes.pipeline.bcbio_config_interaction import grab_yaml
from processes.hiseq.multi_fastq import list_from_multi_fastq_object

class BwaAln(SampleQsubProcess):
    """
    Prepares the bwa aln process
    """

    def __init__(self,config,key=int(-1),process_name='bwa_aln',prev_step=None,bwa_threads=1,ref_fa='/mnt/speed/qc/sequencing/biodata/genomes/Hsapiens/GRCh37/bwa/GRCh37.fa',**kwargs):
        """
        Initializes the  process object.
        """
        if not prev_step is None:
            if prev_step.__class__.__name__ == "ZcatMultiple":
                self.input_fastq = prev_step.r1_uncompressed + ":" + prev_step.r2_uncompressed
                self.bwa_threads = bwa_threads
                self.ref_fa = ref_fa
                input_fastqs = self.input_fastq.split(":")
                SampleQsubProcess.__init__(self,config,key=key,process_name=process_name,input_dir=prev_step.input_dir,output_dir=os.path.join(prev_step.output_dir,"align"),number_tasks=2*prev_step.number_tasks,**kwargs)
                output_sais = []
                count = 1
                for input_fastq in input_fastqs:
                    output_sai = re.sub(prev_step.output_dir,self.output_dir,input_fastq)
                    output_sai = re.sub(".fastq",".sai",output_sai)
                    output_sais.append(output_sai)
                    count += 1
                self.output_sai = ":".join(output_sais)
 
class BwaSampe(SampleQsubProcess):
    """
    Prepares the bwa sampe process for paired end samples only.
    """

    def __init__(self,config,key=int(-1),sample=None,process_name='bwa_sampe',multi_fastq_file=None,ref_fa='/mnt/speed/qc/sequencing/biodata/genomes/Hsapiens/GRCh37/bwa/GRCh37.fa',prev_step=None,project=None,**kwargs):
        """
        Initializes the  process object.
        """
        if not prev_step is None:
            if prev_step.__class__.__name__ == "BwaAln":
                if sample is None:
                    sample = Sample(config,key="dummy_sample_key")
                if sample.__class__.__name__ != "Sample":
                    raise Exception("Trying to start a qcpipeline process on a non-sample.")
                SampleQsubProcess.__init__(self,config,key=key,sample=sample,process_name=process_name,input_dir=prev_step.output_dir,output_dir=prev_step.output_dir,number_tasks=prev_step.number_tasks/2,**kwargs)
                self.project = project
                self.sample_key = sample.key
                self.ref_fa = prev_step.ref_fa
                if not multi_fastq_file is None:
                    self.multi_fastq_file = multi_fastq_file
                    multi_fastq = grab_yaml(self.multi_fastq_file)
                    lane_numbers = list_from_multi_fastq_object(multi_fastq,"lane")
                    flowcells = list_from_multi_fastq_object(multi_fastq,"flowcell")
                    self.lane_number = ":".join(lane_numbers)
                    self.flowcell_key = ":".join(flowcells)

                input_fastqs = prev_step.input_fastq.split(":")
                input_r1_fastqs = input_fastqs[:self.number_tasks]
                input_r2_fastqs = input_fastqs[self.number_tasks:]
                self.input_fastq1 = ":".join(input_r1_fastqs)
                self.input_fastq2 = ":".join(input_r2_fastqs)

                input_sais = prev_step.output_sai.split(":")
                input_r1_sais = input_sais[:self.number_tasks]
                input_r2_sais = input_sais[self.number_tasks:]
                self.input_sai1 = ":".join(input_r1_sais)
                self.input_sai2 = ":".join(input_r2_sais)

                output_sams = []
                for input_r1_fastq in input_r1_fastqs:
                    output_sam = re.sub("_R1.fastq",".sam",input_r1_fastq)
                    output_sams.append(output_sam)
                self.output_sam = ":".join(output_sams)


class SamConversion(SampleQsubProcess):
    """
    Prepares the sam conversion step
    """

    def __init__(self,config,key=int(-1),process_name='sam_conversion',prev_step=None,**kwargs):
        """
        Initializes the  process object.
        """
        if not prev_step is None:
            if prev_step.__class__.__name__ == "BwaSampe":
                SampleQsubProcess.__init__(self,config,key=key,process_name=process_name,input_dir=prev_step.output_dir,output_dir=prev_step.output_dir,number_tasks=prev_step.number_tasks,**kwargs)
                self.input_sam = prev_step.output_sam
                self.output_bam, num = re.subn(r".sam",".bam",self.input_sam)

class SortBam(Bam2BamQsubProcess):
    """
    Prepares the bam sort
    """

    def __init__(self,config,key=int(-1),process_name='sort_bam',prev_step=None,**kwargs):
        """
        Initializes the  process object.
        """
        if not prev_step is None:
            Bam2BamQsubProcess.__init__(self,config,key=key,new_bam_description="sort",process_name=process_name,input_dir=prev_step.output_dir,output_dir=prev_step.output_dir,number_tasks=prev_step.number_tasks,prev_step=prev_step,**kwargs)

class MergeBam(Bam2BamQsubProcess):
    """
    Prepares the bam merge into 1 file for a single sample.
    """

    def __init__(self,config,key=int(-1),process_name='merge_bam',prev_step=None,**kwargs):
        """
        Initializes the process object.
        """
        if not prev_step is None:
            Bam2BamQsubProcess.__init__(self,config,key=key,process_name=process_name,input_dir=prev_step.output_dir,output_dir=prev_step.output_dir,prev_step=prev_step,**kwargs)
            self.output_bam = os.path.join(self.output_dir,self.sample_key + ".sort.bam");#Overwrites because only a single file is needed.

class MarkDuplicates(Bam2BamQsubProcess):
    """
    Prepares the marking of duplicates reads process.
    """

    def __init__(self,config,key=int(-1),process_name='mark_duplicates',prev_step=None,**kwargs):
        """
        Initializes the process object.
        """
        if not prev_step is None:
            Bam2BamQsubProcess.__init__(self,config,key=key,new_bam_description="dedup",process_name=process_name,input_dir=prev_step.output_dir,output_dir=prev_step.output_dir,prev_step=prev_step,**kwargs)
            qc_dir = re.sub(r"align$","qc",self.output_dir)
            if not os.path.isdir(qc_dir) and not re.search('dummy',self.output_dir):
                os.makedirs(qc_dir)
            self.output_metrics = os.path.join(qc_dir,self.sample_key + ".dedup_metrics");

class IndelRealignment(Bam2BamQsubProcess):
    """
    Prepares the indel realignment process.
    """

    def __init__(self,config,key=int(-1),process_name='indel_realignment',ref_fa='/mnt/speed/qc/sequencing/biodata/genomes/Hsapiens/GRCh37/bwa/GRCh37.fa',dbsnp_vcf='/mnt/speed/qc/sequencing/biodata/genomes/Hsapiens/GRCh37/variation/dbsnp_137.vcf',prev_step=None,**kwargs):
        """
        Initializes the process object.
        """
        if not prev_step is None:
            Bam2BamQsubProcess.__init__(self,config,key=key,new_bam_description="realign",process_name=process_name,input_dir=prev_step.output_dir,output_dir=prev_step.output_dir,prev_step=prev_step,**kwargs)
            self.output_intervals = os.path.join(self.output_dir,self.sample_key + ".intervals");
            self.ref_fa = ref_fa
            self.dbsnp_vcf = dbsnp_vcf
            
class BaseRecalibration(Bam2BamQsubProcess):
    """
    Prepares the base_recalibration process.
    """

    def __init__(self,config,key=int(-1),process_name='base_recalibration',ref_fa='/mnt/speed/qc/sequencing/biodata/genomes/Hsapiens/GRCh37/bwa/GRCh37.fa',dbsnp_vcf='/mnt/speed/qc/sequencing/biodata/genomes/Hsapiens/GRCh37/variation/dbsnp_137.vcf',prev_step=None,**kwargs):
        """
        Initializes the process object.
        """
        if not prev_step is None:
            Bam2BamQsubProcess.__init__(self,config,key=key,new_bam_description="recal",process_name=process_name,input_dir=prev_step.output_dir,output_dir=prev_step.output_dir,prev_step=prev_step,**kwargs)
            self.output_recal = os.path.join(self.output_dir,self.sample_key + ".recal");
            self.ref_fa = ref_fa
            self.dbsnp_vcf = dbsnp_vcf

