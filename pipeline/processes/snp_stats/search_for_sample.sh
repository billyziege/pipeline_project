#!/bin/sh
#
#
# (c) 2009 Sun Microsystems, Inc. All rights reserved. Use is subject to license terms.

# This is a simple example of a SGE batch script

# request Bourne shell as shell for job
#$ -S /bin/sh
#$ -cwd
#
#$ -j y
#$ -o /mnt/iscsi_space/zerbeb/qc_pipeline_project/qc_pipeline/sge_records

PIPELINE_INSTALL_PATH=/mnt/iscsi_speed/devel/pipeline
PIPELINE_BIN=$PIPELINE_INSTALL_PATH/all_nodes_050913_env/bin
export LD_LIBRARY_PATH=/mnt/iscsi_speed/devel/Python-2.7.3_2:$LD_LIBRARY_PATH
export PYTHONPATH=$PYTHONPATH:/mnt/iscsi_space/zerbeb/qc_pipeline_project/qc_pipeline
export PATH=/mnt/iscsi_speed/devel/Python-2.7.3_2/bin:$PATH

source $PIPELINE_BIN/activate
source /opt/sge625/sge/default/common/settings.sh

VCF=$1
OUTPUT_DIR=$2

print $VCF

for SEARCH_FILE in `cat /mnt/iscsi_space/zerbeb/qc_pipeline_project/qc_pipeline/processes/snp_stats/search_lists.ls`
do
    node=`python /mnt/iscsi_space/zerbeb/qc_pipeline_project/qc_pipeline/sge_queries/nodes.py`
    qsub -l hostname=$node /mnt/iscsi_space/zerbeb/qc_pipeline_project/qc_pipeline/processes/snp_stats/single_search.sh $VCF $OUTPUT_DIR $SEARCH_FILE
done
