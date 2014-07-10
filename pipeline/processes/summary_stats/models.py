import os
import sys
import re
from manage_storage.disk_queries import disk_usage
from physical_objects.models import Sample
from processes.pipeline.models import Bcbio
from processes.models import GenericProcess, SampleQsubProcess
from processes.summary_stats.extract_stats import store_summary_stats_in_db
from processes.pipeline.bcbio_config_interaction import get_genome_ref
from template.scripts import fill_template

class SummaryStats(SampleQsubProcess):
    """
    This manages and stores the information for the sub-process summary_stats.  Summarry_stats
    is run once bcbio is complete.  It generates the summary_statistics that are kept for run.
    """

    def __init__(self,config,key=int(-1),sample=None,bcbio=None,capture_target_bed=None,process_name='summary_stats',**kwargs):
        """
        Initializes the summary stats process.
        """
        if bcbio is None:
            bcbio = Bcbio(config,key=int(-1))
        if bcbio.__class__.__name__ != "Bcbio":
            raise Exception("Trying to start a summary_stats process on a non-bcbio pipeline.")
        self.capture_target_bed = capture_target_bed
        input_dir = bcbio.output_dir
        output_dir = os.path.join(bcbio.output_dir, "qc/"+str(bcbio.description))
        SampleQsubProcess.__init__(self,config,key=key,sample=sample,input_dir=input_dir,output_dir=output_dir,process_name=process_name,**kwargs)
        self.snp_path = bcbio.snp_path
        self.bam_path = bcbio.analysis_ready_bam_path
        self.systems_file = bcbio.systems_file
        self.ref_path = get_genome_ref(bcbio.sample_file,bcbio.systems_file)
        if self.ref_path is None:
            self.ref_path = "Not_found"
        if config.has_section("Summary stats") and config.has_option("Summary stats","hethom_ext"):
            hethom_filename = self.sample_key + config.get("Summary stats","hethom_ext")
        else:
            hethom_filename = self.sample_key + ".hethom"
        self.hethom_path = os.path.join(self.output_dir,hethom_filename)
        if config.has_section("Summary stats") and config.has_option("Summary stats","indbsnp_ext"):
            indbsnp_filename = self.sample_key + config.get("Summary stats","indbsnp_ext")
        else:
            indbsnp_filename = self.sample_key + ".indbsnp"
        self.indbsnp_path = os.path.join(self.output_dir,indbsnp_filename)
        if config.has_section("Summary stats") and config.has_option("Summary stats","hs_metrics_ext"):
            hs_metrics_filename = self.sample_key + config.get("Summary stats","hs_metrics_ext")
        else:
            hs_metrics_filename = self.sample_key + ".hs_metrics"
        self.hs_metrics_path = os.path.join(self.output_dir,hs_metrics_filename)
        if config.has_section("Summary stats") and config.has_option("Summary stats","bamtools_stats_ext"):
            bamtools_stats_filename = self.sample_key + config.get("Summary stats","bamtools_stats_ext")
        else:
            bamtools_stats_filename = self.sample_key + ".bamtools_stats"
        self.bamtools_stats_path = os.path.join(self.output_dir,bamtools_stats_filename)
        if config.has_section("Summary stats") and config.has_option("Summary stats","vcfstats_stats_ext"):
            vcfstats_stats_filename = self.sample_key + config.get("Summary stats","vcfstats_stats_ext")
        else:
            vcfstats_stats_filename = self.sample_key + ".vcfstats_stats"
        self.vcfstats_stats_path = os.path.join(self.output_dir,vcfstats_stats_filename)
        self.summary_stats_path = os.path.join(self.output_dir,"summary-stats.csv")
        #Stats for this process
        self.hom = None
        self.het = None
        self.variants_total = None
        self.hethom_ratio = None
        self.total_reads = None
        self.percent_aligned = None
        self.percentage_duplicates = None
        self.insert_size = None
        self.percentage_on_target_bases = None
        self.percentage_near_target_bases = None
        self.mean_target_coverage = None
        self.percentage_with_at_least_10x_coverage = None
        self.percentage_0x_coverage = None
        self.percentage_in_db_snp = None
        self.ts_tv_ratio = None

    def __is_complete__(self,configs):
        """
        Since concordance search is an optional sub-process, this function checks for
        both the completeness of the self concordance program and the necessity,
        and if necessary the completeness, of the concordance search.  Then, once 
        complete, relevant statistics are stored.
        """
        if GenericProcess.__is_complete__(self):
            return True
        elif not os.path.isfile(self.complete_file):
            return False
        store_summary_stats_in_db(self)
        self.__finish__()
        return True

