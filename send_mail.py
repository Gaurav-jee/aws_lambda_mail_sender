import smtplib
import io
import os
import boto3
import pandas as pd
import mimetypes
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email import encoders
from email.message import Message
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from datetime import date


'''
:summary:  - sends a mail to inform about the status of the process
:param1:   - message containing the status and required data
:return:   - success or exception messages
'''
def send_email_with_message(message, ucr_attachment):
    
    username = os.environ["mail_username"]
    password = os.environ["mail_password"]
    host = os.environ["HOST"]
    port = os.environ["PORT"]
    
    #mailing details
    mail_from = 'digix.support@kfintech.com' 
    mail_to = ['santosh.kencha@kfintech.com']
    mail_cc = ['hemanth.m@kfintech.com', 'madhusudan.gattu@kfintech.com', 'vasanth.santhanam@kfintech.com', 'ramkaushik.kadha@kfintech.com', 'v-sattwik.palai@kfintech.com', 'suryakanta.nanda@kfintech.com', 'gaurav.jee@kfintech.com']
    # mail_cc = []
    
    today = date.today()
    d2 = today.strftime("%B %d, %Y")
    filename = 'UTI UCR - ({}).csv'.format(d2)
    
    msg = MIMEMultipart()
    msg['From'] = mail_from
    msg['To'] = ','.join(mail_to)
    msg['Cc'] = ",".join(mail_cc)
    msg['Subject'] = "UTI Daily UCR as on - ({})".format(d2)
    
    body = ('Hi team, \n\n{} \n \nThank you, \nDigix Team.').format(message)
    body = MIMEText(body) 
    msg.attach(body)
    
    # write DataFrame to CSV file
    ucr_attachment_csv = ucr_attachment.to_csv(index=False)

    csv_part = MIMEApplication(ucr_attachment_csv)
    csv_part.add_header('Content-Disposition', 'attachment', filename=filename)
    msg.attach(csv_part)
    
    try:
        server = smtplib.SMTP(host, port)
        server.ehlo()
        server.starttls()
        server.login(username, password)
        server.sendmail(mail_from, [mail_to] + mail_cc, msg.as_string())
        server.close()
        return "Mail Delivered!"
    except Exception as ex:
        print(ex)
        return ex
        



    