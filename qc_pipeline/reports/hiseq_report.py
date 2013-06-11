import ConfigParser
import sys
import os
from mockdb.initiate_mockdb import initiate_mockdb

def hiseq_report(mockdb):
    seq_run_flowcell_dict = mockdb['SequencingRun'].__attribute_value_to_object_dict__('flowcell_key')
    lane_barcode_dict = mockdb['Barcode'].__attribute_value_to_object_dict__('lane_key')
    print "start_datetime,end_datetime,Flowcell ID,Machine ID,sampleids-barcodes,Total Reads,Undetermined Reads (not included in Total),% PF,% of >= Q30 Bases (PF),barcodes-reads,path"  
    for lane_key,lane in mockdb['Lane'].objects.iteritems():
        try :
            flowcell = mockdb['Flowcell'].objects[lane.flowcell_key]
        except:
            continue
        try:
            seq_runs = seq_run_flowcell_dict[flowcell.key]
        except:
            continue
        if len(seq_runs) > 1:
            raise Exception
        seq_run = seq_runs[0]
        machine_key = seq_run.machine_key
        begin_timestamp = seq_run.begin_timestamp
        end_timestamp = seq_run.end_timestamp
        string = str(begin_timestamp) + ',' + str(end_timestamp) + ',' + str(flowcell.key)
        string += ',' + str(machine_key) + ','
        sb = ""
        for barcode in lane_barcode_dict[lane_key]:
            if sb != "":
                sb += ":"
            sb += barcode.sample_key + "-" + barcode.index
        string += sb + ',' + str(lane.total_reads) + ',' + str(lane.undetermined_reads)
        string += ',' + str(lane.percentage_pf)
        string += ',' + str(lane.percentage_above_q30) + ','
        br = ""
        for barcode in lane_barcode_dict[lane_key]:
            if br != "":
                br += ":"
            br += barcode.index + "-" + str(barcode.reads)
        string += br + ',' + seq_run.output_dir
        print string
    return 1


if __name__ == "__main__":
    config = ConfigParser.ConfigParser()
    config.read('/mnt/iscsi_space/zerbeb/qc_pipeline_project/qc_pipeline/config/qc.cfg')
    mockdb = initiate_mockdb(config)
    hiseq_report(mockdb)
