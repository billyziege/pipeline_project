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
        if expected_available < needed:
            return False
        my_space = self.limit - self.my_use
        if my_space < 0:
            return False
        return True

    def __is_full__(self,extra):
       return self.available < extra
