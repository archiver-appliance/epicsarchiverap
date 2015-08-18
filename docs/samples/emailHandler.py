'''Small module to handle sending emails
Configuration is JSON and is imported from a file specified by the environment variable ARCHAPPL_NAGIOS_EMAIL_CONFIG
We expect these variables in the dict
from - The from address
to - A list of destination email addresses
smtphost - Your local SMTP host.

Note Nagios already has a lot of the email functionality so you do not need this module at all.
These are samples that you can use  
'''
import os
import smtplib
import json
from email.mime.text import MIMEText

# Read config from ARCHAPPL_NAGIOS_EMAIL_CONFIG
emailConfig = []
with open(os.getenv('ARCHAPPL_NAGIOS_EMAIL_CONFIG', '/arch/tools/config/archappl_email_config')) as f:
    emailConfig = json.load(f)

def sendEmail(subject, message, pvNameList):
    '''Send an email using the specified subject, message and pvNameList
    To and from and other configuration are stored as variables in this module
    '''
    msg = MIMEText(message + '\n' + '\n'.join(pvNameList))
    msg['Subject'] = '' + subject
    msg['From'] = emailConfig['from']
    msg['To'] = emailConfig['to'][0]
    s = smtplib.SMTP(emailConfig['smtphost'])
    s.sendmail(emailConfig['from'], emailConfig['to'], msg.as_string())
    s.quit()
