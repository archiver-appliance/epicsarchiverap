#!/usr/bin/env python

# This script adds a EPICS archiver appliance MySQL connection pool to a Tomcat context.xml
# The location of context.xml is determined by the ${TOMCAT_HOME} environment variable
# The parameters for the connection pool are determined by the ${MYSQL_CONNECTION_STRING} environment variable  


import sys
import os
import xml.dom.minidom
import urlparse
import shutil

mysql_connection_string = os.getenv('MYSQL_CONNECTION_STRING')
if not mysql_connection_string:
	print 'The environment variable MYSQL_CONNECTION_STRING is not defined. Please define using export MYSQL_CONNECTION_STRING="--user=archappl --password=archappl --database=archappl"'
	sys.exit(1)

tomcatHome = os.getenv("TOMCAT_HOME")
if tomcatHome == None:
	print "We determine the location of context.xml using the environment variable TOMCAT_HOME which does not seem to be set."
	sys.exit(1)

connpoolparams = {}
for mysqlparam in mysql_connection_string.split():
	paramnv = mysqlparam.split('=')
	paramame = paramnv[0].replace('--', '')
	paramval = paramnv[1]
	connpoolparams[paramame] = paramval

if 'user' not in connpoolparams:
	print 'Cannot determine the user from ", mysql_connection_string, ". Please define like so "--user=archappl --password=archappl --database=archappl"'
	sys.exit(1)

if 'password' not in connpoolparams:
	print 'Cannot determine the user from ", mysql_connection_string, ". Please define like so "--user=archappl --password=archappl --database=archappl"'
	sys.exit(1)

if 'database' not in connpoolparams:
	print 'Cannot determine the user from ", mysql_connection_string, ". Please define like so "--user=archappl --password=archappl --database=archappl"'
	sys.exit(1)

user=connpoolparams['user']
pwd=connpoolparams['password']
db=connpoolparams['database']

tomcatContextXML = tomcatHome + '/conf/context.xml'
serverdom = xml.dom.minidom.parse(tomcatContextXML);
resources = serverdom.getElementsByTagName('Resource')
for resource in resources:
	if resource.getAttribute('name') == 'jdbc/archappl':
		print "Connection pool jdbc/archappl is already defined"
		sys.exit(0)


connpool =  serverdom.createElement('Resource')
connpool.setAttribute('name',"jdbc/archappl")
connpool.setAttribute('auth',"Container")
connpool.setAttribute('type',"javax.sql.DataSource")
connpool.setAttribute('factory',"org.apache.tomcat.jdbc.pool.DataSourceFactory")
connpool.setAttribute('testWhileIdle',"true")
connpool.setAttribute('testOnBorrow',"true")
connpool.setAttribute('testOnReturn',"false")
connpool.setAttribute('validationQuery',"SELECT 1")
connpool.setAttribute('validationInterval',"30000")
connpool.setAttribute('timeBetweenEvictionRunsMillis',"30000")
connpool.setAttribute('maxActive',"10")
connpool.setAttribute('minIdle',"2")
connpool.setAttribute('maxWait',"10000")
connpool.setAttribute('initialSize',"2")
connpool.setAttribute('removeAbandonedTimeout',"60")
connpool.setAttribute('removeAbandoned',"true")
connpool.setAttribute('logAbandoned',"true")
connpool.setAttribute('minEvictableIdleTimeMillis',"30000")
connpool.setAttribute('jmxEnabled',"true")
connpool.setAttribute('driverClassName',"com.mysql.jdbc.Driver")
connpool.setAttribute('url',"jdbc:mysql://localhost:3306/" + connpoolparams['database'])
connpool.setAttribute('username',connpoolparams['user'])
connpool.setAttribute('password',connpoolparams['password'] )

serverdom.getElementsByTagName('Context')[0].appendChild(connpool)

with open(tomcatContextXML, 'w') as file: 
	file.write(serverdom.toprettyxml())

