import os
import re
import sys
from manage_storage.disk_queries import disk_usage
from physical_objects.hiseq.models import Sample
from processes.models import SampleQsubProcess
from template.scripts import fill_template
from sge_email.scripts import send_email

class UnifiedGenotyper(SampleQsubProcess):
    """
    Prepares the unified genotyper process
    """

    def __init__(self,config,key=int(-1),process_name='unified_genotyper',ref_fa='/mnt/speed/qc/sequencing/biodata/genomes/Hsapiens/GRCh37/bwa/GRCh37.fa',dbsnp_vcf='/mnt/speed/qc/sequencing/biodata/genomes/Hsapiens/GRCh37/variation/dbsnp_137.vcf',prev_step=None,**kwargs):
        """
        Initializes the process object.
        """
        if not prev_step is None:
            output_dir = re.sub(re.sub(r"align$","gatk_ug",prev_step.output_dir))
            self.input_bam = prev_step.output_bam;
            self.output_vcf = os.path.join(output_dir,self.sample_key + ".vcf");
            SampleQsubProcess.__init__(self,config,key=key,new_bam_description="recal",process_name=process_name,input_dir=prev_step.output_dir,output_dir=output_dir,**kwargs)
            self.ref_fa = ref_fa
            self.dbsnp_vcf = dbsnp_vcf
            qc_dir = re.sub(r"gatk_ug$","qc",self.output_dir)
            if not os.path.isdir(qc_dir) and not re.search('dummy',self.output_dir):
                os.makedirs(qc_dir)
            self.output_metrics = os.path.join(qc_dir,self.sample_key + ".ug_metrics");

