def get_location_dictionary_from_config(config):
    location_options = config.get('Location_options','list').split(",")
    location_dirs = {}
    for location in location_options:
        directory = config.get(location + '_directories','dir')
        location_dirs.update({location:directory})
    return location_dirs
