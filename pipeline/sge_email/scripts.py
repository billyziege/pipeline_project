import smtplib
from email.MIMEMultipart import MIMEMultipart
from email.MIMEBase import MIMEBase
from email.MIMEText import MIMEText
from email.Utils import COMMASPACE, formatdate
from email import Encoders
from sge_email.models import SGEEmailObject

def send_email(subject,message,recipients=None,files=[]):
    sgeemail = SGEEmailObject(subject=subject,message=message,recipients=recipients)
    #fp = open(filename, 'rb')
    # Create a text/plain message
    msg = MIMEMultipart
    msg['Subject'] = sgeemail.subject
    msg['Date'] = formatdate(localtime=True)
    msg['From'] = sgeemail.usrname + '@' + sgeemail.domain
    msg['To'] = COMMASPACE.join(sgeemail.recipients)

    msg.attach( MIMEText(sgeemail.caveat + sgeemail.message + sgeemail.salutation) )

    for f in files:
        part = MIMEBase('application', "octet-stream")
        part.set_payload( open(f,"rb").read() )
        Encoders.encode_base64(part)
        part.add_header('Content-Disposition', 'attachment; filename="%s"' % os.path.basename(f))
        msg.attach(part)

    s = smtplib.SMTP_SSL(sgeemail.host,465)
    s.set_debuglevel(1)
    s.login(sgeemail.usrname, sgeemail.password)
    s.sendmail(sgeemail.usrname + '@' + sgeemail.domain, sgeemail.recipients, msg.as_string())
    s.quit()

if __name__ == '__main__':
    send_email("Test","Just a test.\n")
