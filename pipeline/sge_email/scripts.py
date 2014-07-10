import sys
import os
import smtplib
import argparse
from email.MIMEMultipart import MIMEMultipart
from email.MIMEBase import MIMEBase
from email.MIMEText import MIMEText
from email.Utils import COMMASPACE, formatdate
from email import Encoders
from sge_email.models import SGEEmailObject

def send_email(subject,message,recipients=None,files=[],add_text=None):
    sgeemail = SGEEmailObject(subject=subject,message=message,recipients=recipients)
    #fp = open(filename, 'rb')
    # Create a text/plain message
    msg = MIMEMultipart()
    msg['Subject'] = sgeemail.subject
    msg['Date'] = formatdate(localtime=True)
    msg['From'] = sgeemail.usrname + '@' + sgeemail.domain
    msg['To'] = COMMASPACE.join(sgeemail.recipients)

    if add_text is None:
        msg.attach( MIMEText(sgeemail.caveat + sgeemail.message + sgeemail.salutation) )
    else:
    	msg.attach( MIMEText(sgeemail.caveat + sgeemail.message + add_text + sgeemail.salutation) )

    for f in files:
        part = MIMEBase('application', "octet-stream")
        part.set_payload( open(f,"rb").read() )
        Encoders.encode_base64(part)
        part.add_header('Content-Disposition', 'attachment; filename="%s"' % os.path.basename(f))
        msg.attach(part)

    try:
        s = smtplib.SMTP_SSL(sgeemail.host,465)
        s.set_debuglevel(1)
        s.login(sgeemail.usrname, sgeemail.password)
        s.sendmail(sgeemail.usrname + '@' + sgeemail.domain, sgeemail.recipients, msg.as_string())
        s.quit()
    except:
        print "Could not send email:\n{0}\n{1}\n".format(subject,message)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Sends an sge email with salutation caveat and the like')
    parser.add_argument('-s', '--subject', dest='subject',default='Test',type=str, help='The email subject field')
    parser.add_argument('-m', '--message', dest='message',default='Just a test.',type=str, help='The email message field')
    parser.add_argument('--message_file', dest='message_file',default=None,type=str, help='Overwrites message.  If a message file is provided, then the text in the file becomes the message.')
    parser.add_argument('-r','--recipients', dest='recipients',default='zerbeb@humgen.ucsf.edu',type=str, help='The to field of the email.')
    parser.add_argument('-a','--attachments', dest='attachments',default=None,type=str, help='A comma separated (no space) list of files to attach.')
    args = parser.parse_args()
    if args.message_file is not None:
        with open(args.message_file,"r") as f:
            args.message = "\n".join(f.readlines())
    if args.attachments is not None:
        files = args.attachments.split(',')
    else:
        files = []
    send_email(args.subject,args.message,recipients=args.recipients,files=files)
