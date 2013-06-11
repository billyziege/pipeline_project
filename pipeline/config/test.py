import ConfigParser

config = ConfigParser.ConfigParser()
config.read('/home/zerbeb/homemade_programs/qc_pipeline_project/qc_pipeline/config/qc_on_orion.cfg')
print config.get('Filenames', 'demultiplex')
print config.get('Common_directories', 'template')
print config.get('Location_options', 'list').split(',')
