from manage_storage.disk_queries import disk_available
class StorageDevice:
    
    def __init__(self,directory,name=None,**kwargs):
        self.directory = directory
        if name == None:
            self.name = directory
        else:
            self.name = name
        self.available = int(disk_available(directory))
        self.limit = int(0)
        self.my_use = int(0)
        self.waiting = int(0)


    def __is_available__(self,needed):
        if self.available > needed:
           my_space = self.limit - self.my_use
           if my_space > 0:
               return True
        return False
