import smtplib
from email.mime.text import MIMEText
from sge_email.models import SGEEmailObject

def send_email(subject,message,recipients=None):
    sgeemail = SGEEmailObject(subject=subject,message=message,recipients=recipients)
    #fp = open(filename, 'rb')
    # Create a text/plain message
    msg = MIMEText(sgeemail.caveat + sgeemail.message + sgeemail.salutation)
    COMMASPACE = ', '
    msg['Subject'] = sgeemail.subject
    msg['From'] = sgeemail.usrname + '@' + sgeemail.domain
    msg['To'] = COMMASPACE.join(sgeemail.recipients)

    print sgeemail.host
    s = smtplib.SMTP_SSL(sgeemail.host,465)
    s.set_debuglevel(1)
    #s.ehlo()
    #s.starttls()
    #s.ehlo()
    #s.connect(sgeemail.host,465)
    print sgeemail.usrname
    print sgeemail.password
    s.login(sgeemail.usrname, sgeemail.password)
    #s.helo(sgeemail.ip)
    print sgeemail.usrname + '@' + sgeemail.domain
    s.sendmail(sgeemail.usrname + '@' + sgeemail.domain, sgeemail.recipients, msg.as_string())
    s.quit()

if __name__ == '__main__':
    send_email("Test","Just a test.\n")
