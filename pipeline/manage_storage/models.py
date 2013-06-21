from manage_storage.disk_queries import disk_available
class StorageDevice:
    
    def __init__(self,directory,name=None,limit=None,**kwargs):
        self.directory = directory
        if name == None:
            self.name = directory
        else:
            self.name = name
        self.available = int(disk_available(directory))
        if limit is None:
            self.limit = self.available
        else:
            self.limit = limit
        self.my_use = int(0)
        self.waiting = int(0)


    def __is_available__(self,needed):
        expected_available = self.available - self.my_use
        if int(expected_available) < int(needed):
            return False
        my_space = int(self.limit) - int(self.my_use)
        if int(my_space) < int(0):
            return False
        return True

    def __is_full__(self,extra):
       return int(self.available) < int(extra)
