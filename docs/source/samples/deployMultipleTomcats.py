#!/usr/bin/env python3

# This script deploys a archiver appliance build onto a appliance into four separate JVM's as per the documentation.
# The artifacts from the build are four war files that are independent of each other. 
# One can choose to deploy all of these on one JVM; however, as the archiver appliance is a fairly memory intensive application; this can adversely impact GC on the single JVM.
# Also, one does not want a large user retrieval to affect the engine or ETL.
# Therefore, there are some advantages to deploying each war file on a separate JVM.

# This script uses TOMCAT_HOME to determine the source of the Tomcat distribution.
# It then makes four subfolders in the specified folder, one each for the mgmt, engine, etl and retrieval webapps.
# It copies over the necessary content from the source into each of these subfolders and adjusts the configuration as necessary.
# It then deploys the war files into the appropriate tomcats.
# The war files are assumed to be in the parent folder; however you can use an option to override this. 

# This is the just the deployment step; you can then have individual /etc/init.d scripts (or use commons daemon) to start the four webapps.
# So your deployment script should be something like 1) Stop webapps 2) Deploy webapps using this script 3) Start webapps.

# The appliance port configuration is determined from appliances.xml as specified in the ARCHAPPL_APPLIANCES environment variable.
# The identity for this appliance is determined from the ARCHAPPL_MYIDENTITY environment variable and ise used to lookup the parameters for this appliance in appliances.xml.
# The tomcat start/stop ports are set by incrementing the port specified in the source for each of the webapps.

# To generate start and stop script use something like this
# export CATALINA_HOME=$TOMCAT_HOME
# DEPLOY_DIR=/opt/local/ArchiverAppliance/tomcats
# The stop sequences....
# export CATALINA_BASE=${DEPLOY_DIR}/mgmt; ${CATALINA_HOME}/bin/catalina.sh stop 
# export CATALINA_BASE=${DEPLOY_DIR}/engine; ${CATALINA_HOME}/bin/catalina.sh stop 
# export CATALINA_BASE=${DEPLOY_DIR}/etl; ${CATALINA_HOME}/bin/catalina.sh stop 
# export CATALINA_BASE=${DEPLOY_DIR}/retrieval; ${CATALINA_HOME}/bin/catalina.sh stop 
# The start sequences
# export CATALINA_BASE=${DEPLOY_DIR}/mgmt; ${CATALINA_HOME}/bin/catalina.sh start
# export CATALINA_BASE=${DEPLOY_DIR}/engine; ${CATALINA_HOME}/bin/catalina.sh start
# export CATALINA_BASE=${DEPLOY_DIR}/etl; ${CATALINA_HOME}/bin/catalina.sh start
# export CATALINA_BASE=${DEPLOY_DIR}/retrieval; ${CATALINA_HOME}/bin/catalina.sh start



import sys
import os
import xml.dom.minidom
import urllib.parse
import shutil

def deployMultipleTomcats(destFolder):
    tomcatHome = os.getenv("TOMCAT_HOME")
    if tomcatHome == None:
        print("We determine the source Tomcat distribution using the environment variable TOMCAT_HOME which does not seem to be set.")
        sys.exit(1)
    thisAppliance = os.getenv("ARCHAPPL_MYIDENTITY")
    if thisAppliance == None:
        print("We determine the identity of this appliance using the environment variable ARCHAPPL_MYIDENTITY which does not seem to be set.")
        sys.exit(1)
    appliancesXML = os.getenv("ARCHAPPL_APPLIANCES")
    if appliancesXML == None:
        print("We determine the location of the appliances.xml file using the environment variable ARCHAPPL_APPLIANCES which does not seem to be set.")
        sys.exit(1)

    print("Using\n\ttomcat installation at", tomcatHome, "\n\tto generate deployments for appliance", thisAppliance, "\n\tusing configuration info from", appliancesXML, "\n\tinto folder", destFolder)
    
    # Parse the tomcat/conf/server.xml file and determine the stop start port
    # We start incrementing and use this port+1 for each of the new webapps
    tomcatServerConfSrc = os.path.join(tomcatHome, 'conf', 'server.xml')
    serverdom = xml.dom.minidom.parse(tomcatServerConfSrc)
    serverStopStartPort = serverdom.getElementsByTagName('Server').item(0).getAttribute('port')
    if int(serverStopStartPort) == 8005:
    	print("The start/stop port is the standard Tomcat start/stop port. Changing it to something else random - 16000")
    	serverStopStartPort='16000'
    newServerStopStartPort = int(serverStopStartPort)+1
    print('The stop/start ports for the new instance will being at ', newServerStopStartPort)
    
    # Parse the appliances.xml and determine the ports for the HTTP listeners
    appliancesXMLDOM =  xml.dom.minidom.parse(appliancesXML)
    appliances = appliancesXMLDOM.getElementsByTagName('appliance')
    for appliance in appliances:
        identity = appliance.getElementsByTagName('identity').item(0).firstChild.data
        if identity != thisAppliance:
            # print "Skipping config for", appliance, " looking for ", thisAppliance
            continue
        httplistenerports = {}
        mgmtUrl = appliance.getElementsByTagName('mgmt_url').item(0).firstChild.data
        engineUrl = appliance.getElementsByTagName('engine_url').item(0).firstChild.data
        etlUrl = appliance.getElementsByTagName('etl_url').item(0).firstChild.data
        retrievalUrl = appliance.getElementsByTagName('retrieval_url').item(0).firstChild.data
        httplistenerports['mgmt'] = urllib.parse.urlparse(mgmtUrl).port
        httplistenerports['engine'] = urllib.parse.urlparse(engineUrl).port
        httplistenerports['etl'] = urllib.parse.urlparse(etlUrl).port
        httplistenerports['retrieval'] = urllib.parse.urlparse(retrievalUrl).port
        
        for app in ['mgmt', 'engine', 'etl', 'retrieval']:
            # Delete any existing subfolders if any and make new ones.
            subFolder = os.path.join(destFolder, app)
            if os.access(subFolder, os.W_OK):
                print("Removing ", subFolder)
                shutil.rmtree(subFolder)
            print("Generating tomcat folder for ", app, " in location", subFolder)
            os.makedirs(subFolder)
            # See http://kief.com/running-multiple-tomcat-instances-on-one-server.html for the steps we are using here.
            shutil.copytree(os.path.join(tomcatHome, 'conf'), os.path.join(subFolder, 'conf'))
            shutil.copytree(os.path.join(tomcatHome, 'webapps'), os.path.join(subFolder, 'webapps'))
            os.makedirs(os.path.join(subFolder, 'logs'))
            os.makedirs(os.path.join(subFolder, 'temp'))
            os.makedirs(os.path.join(subFolder, 'work'))
            
            # print 'StopStart for', app, ' is', newServerStopStartPort, "and the HTTP listener port as determined from appliances.xml is", httplistenerports[app]
            newServerDom = xml.dom.minidom.parse(os.path.join(subFolder, 'conf', 'server.xml'))
            newServerDom.getElementsByTagName('Server').item(0).setAttribute('port', str(newServerStopStartPort))
            # Find the 'Connector' whose 'protocol' is 'HTTP/1.1'
            haveSetConnector = False
            for httplistenerNode in newServerDom.getElementsByTagName('Connector'):
                if httplistenerNode.hasAttribute('protocol') and httplistenerNode.getAttribute('protocol') == 'HTTP/1.1':
                    httplistenerNode.setAttribute('port', str(httplistenerports[app]))
                    haveSetConnector = True
                else:
                    print("Commenting connector with protocol ", httplistenerNode.getAttribute('protocol'), ". If you do need this connector, you should un-comment this.")
                    comment = httplistenerNode.ownerDocument.createComment(httplistenerNode.toxml())
                    httplistenerNode.parentNode.replaceChild(comment, httplistenerNode)
            if haveSetConnector == False:
                raise AssertionError("We have not set the HTTP listener port for " + app)
            

            with open(os.path.join(subFolder, 'conf', 'server.xml'), 'w') as file: 
                newServerDom.writexml(file)
            newServerStopStartPort =  newServerStopStartPort+1

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: ", sys.argv[0], "<DeploymentFolder>")
        sys.exit(1)
    deployMultipleTomcats(sys.argv[1])



