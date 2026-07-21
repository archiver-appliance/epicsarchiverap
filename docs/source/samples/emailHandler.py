'''Small module to handle sending emails
Configuration is JSON and is imported from a file specified by the environment variable ARCHAPPL_NAGIOS_EMAIL_CONFIG
We expect these variables in the dict
from - The from address
to - A list of destination email addresses
smtphost - Your local SMTP host.
port - Your local SMTP port
use_ssl - Whether to use SSL from the start
use_tls - Whether to upgrade the connection to secure when connected
username - username for SMTP authentication
password" - password for SMTP authentication

This is an example of the JSON configuration file,
{
  "from": "user@example.com",
  "to": [
    "admin1@example.com",
    "admin2@example.com"
  ],
  "smtphost": "smtp.example.com",
  "port": 587,
  "use_ssl": false,
  "use_tls": true,
  "username": "your_username",
  "password": "your_password"
}

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

    s = None
    if emailConfig['use_ssl']:
        # Used when SSL is required from the beginning of the connection and using starttls() is not appropriate
        s = smtplib.SMTP_SSL(emailConfig['smtphost'], emailConfig['port'])
    else:
        s = smtplib.SMTP(emailConfig['smtphost'], emailConfig['port'])

    if emailConfig['use_tls']:
        # Put the SMTP connection in TLS (Transport Layer Security) mode
        s.starttls()

    username = emailConfig['username']
    password = emailConfig['password']
    if username and password:
        # Log in on an SMTP server that requires authentication
        s.login(username, password)

    s.sendmail(emailConfig['from'], emailConfig['to'], msg.as_string())
    s.quit()
