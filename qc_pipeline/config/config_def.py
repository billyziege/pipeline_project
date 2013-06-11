import ConfigParser

config = ConfigParser.ConfigParser()

# When adding sections or items, add them in the reverse order of
# how you want them to be displayed in the actual file.
# In addition, please note that using RawConfigParser's and the raw
# mode of ConfigParser's respective set functions, you can assign
# non-string values to keys internally, but will receive an error
# when attempting to write to a file or when you get it in non-raw
# mode. SafeConfigParser does not allow such assignments to take place.
config.add_section('Storage')
config.set('Storage', 'buffer', '100000000')
config.set('Storage', 'expected_fastq_size', '20000000')
config.set('Storage', 'limit', '20000000000')
config.set('Storage', 'needed', '500000000')

config.add_section('SGE')
config.set('SGE', 'do_not_use_nodes', 'node2,node5,node6,node22')

config.add_section('Template_files')
config.set('Template_files', 'system', 'system_old.template')
config.set('Template_files', 'sample', 'sample_old.template')
config.set('Template_files', 'bcbio', 'bcbio_old.template')

config.add_section('Filenames')
config.set('Filenames', 'casava_finished', 'bcl2fastq.complete')
config.set('Filenames', 'demultiplex', 'Demultiplex_Stats.htm')
config.set('Filenames', 'basecalling_complete', 'Basecalling_Netcopy_complete.txt')
config.set('Filenames', 'basecalling_initialized', 'Config/HiSeqControlSoftware.Options.cfg')

config.add_section('Common_directories')
config.set('Common_directories', 'bcbio_upload', '/mnt/coldstorage/open-transfers/msbp_analysed_exomes')
config.set('Common_directories', 'casava_output', '/mnt/coldstorage/open-transfers/client_pickup')
config.set('Common_directories', 'hiseq_output', '/mnt/coldstorage/hiseq-data')
config.set('Common_directories', 'template', '%(project)s/storage/templates')
config.set('Common_directories', 'mockdb', '%(project)s/storage')
config.set('Common_directories', 'classes', '%(project)s/mockdb,%(project)s/processes,%(project)s/processes/hiseq,%(project)s/processes/zcat,%(project)s/processes/pipeline')
config.set('Common_directories', 'project', '/mnt/iscsi_speed/zerbeb/qc_pipeline_project/qc_pipeline')

config.add_section('Space_directories')
config.set('Space_directories', 'bcbio_output', '%(dir)s/cron_pipeline')
config.set('Space_directories', 'dir', '/mnt/iscsi_space/zerbeb')

config.add_section('Speed_directories')
config.set('Speed_directories', 'bcbio_output', '%(dir)s/cron_pipeline')
config.set('Speed_directories', 'dir', '/mnt/iscsi_speed/zerbeb')

config.add_section('Location_options')
config.set('Location_options', 'list', 'Space,Speed')


# Writing our configuration file to 'example.cfg'
with open('qc.cfg', 'wb') as configfile:
    config.write(configfile)
