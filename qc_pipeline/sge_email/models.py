class SGEEmailObject:
    
    def __init__(self,subject=None,message=None,*args,**kwargs):
        self.sender = 'zerbeb@humgen.ucsf.edu'
        self.sender_name = 'Brandon Zerbe'
        self.recipients = ['zerbeb@humgen.ucsf.edu','tanglf@humgen.ucsf.edu','KvaleM@humgen.ucsf.edu']
        self.password = 'P@ssword'
        self.usrname = 'ccrjobs'
        self.domain = 'anesthesia.ucsf.edu'
        self.main_name = 'VCF Pipeline'
        self.host = 'mail.ucsf.edu'
        self.message = message
        self.subject = subject
        self.ip = '128.218.92.6'
        caveat  = "This is an automatically generated message"
        caveat += " sent by the automatic variant-calling pipeline.  "
        caveat += "Please do not"
        caveat += " reply to this email.  Instead, if you have a concern,"
        caveat += " contact " + self.sender_name + " via "
        caveat += self.sender + ".\n\n";
        self.caveat = caveat
        self.salutation = "Regards,\n  --" + self.main_name
