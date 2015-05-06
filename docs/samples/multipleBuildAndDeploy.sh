#!/bin/bash

# Single script to build, deploy, start and stop the archiver appliance on multiple tomcat instances
# Set PROJECT_DIR to your workspace/project
# Set DEPLOY_DIR to where you'd want the tomcat distribution to be copied and modified.
#
# At a high level, we copy from TOMCAT_HOME the folders necessary for four tomcat instances, one each for mgmt, engine, etl and retrieval
# We deploy the war files and then use jsvc to start/stop these instances
# Note that jsvc does not get built by default. See the instructions at http://tomcat.apache.org/tomcat-7.0-doc/setup.html to build the jsvc shipped with tomcat.
#
# Calling this script without any arguments deploys and starts the instances
# Passing a single argument "stop" stops the deployed instances.


if [ -z "$PROJECT_DIR" ]
then
   export PROJECT_DIR=/scratch/Work/workspace/epicsarchiverap
fi

if [ -z "$DEPLOY_DIR" ]
then
    export DEPLOY_DIR=/scratch/LargeDisk/tomcats
fi


# Inherit CATALINA_HOME from TOMCAT_HOME
export CATALINA_HOME=$TOMCAT_HOME
if [[ ! -d ${CATALINA_HOME} ]]
then
    echo "Unable to determine the source of the tomcat distribution"
    exit 1
fi

# Set the identity of this appliance
if [ -z "$ARCHAPPL_MYIDENTITY" ]
then
   export ARCHAPPL_MYIDENTITY="archappl0"
fi

if [ -z "$ARCHAPPL_SITEID" ]
then
   export ARCHAPPL_SITEID="slacdev"
fi
# Set the location of short term, medium term and long term stores...
if [ -z "$ARCHAPPL_SHORT_TERM_FOLDER" ]
then
    export ARCHAPPL_SHORT_TERM_FOLDER=/dev/shm/test
fi
if [ -z "$ARCHAPPL_MEDIUM_TERM_FOLDER" ]
then
    export ARCHAPPL_MEDIUM_TERM_FOLDER=/scratch/LargeDisk/ArchiverStore/MTS
fi
if [ -z "$ARCHAPPL_LONG_TERM_FOLDER" ]
then
    export ARCHAPPL_LONG_TERM_FOLDER=/scratch/LargeDisk/ArchiverStore/LTS
fi

# Pick up the appliances.xml from the PROJECT_DIR and ARCHAPPL_SITEID
export ARCHAPPL_APPLIANCES=${PROJECT_DIR}/src/sitespecific/${ARCHAPPL_SITEID}/classpathfiles/appliances.xml

if [[ ! -f ${ARCHAPPL_APPLIANCES} ]]
then
    echo "Unable to find appliance.xml at ${ARCHAPPL_APPLIANCES}"
    exit 1
fi


function startTomcatAtLocation() { 
    if [ -z "$1" ]; then echo "startTomcatAtLocation called without any arguments"; exit 1; fi
    export CATALINA_BASE=$1
    echo "Starting tomcat at location ${CATALINA_BASE}"
    pushd ${CATALINA_BASE}/logs
    ${CATALINA_HOME}/bin/jsvc \
        -server \
        -cp ${CATALINA_HOME}/bin/bootstrap.jar:${CATALINA_HOME}/bin/tomcat-juli.jar \
        ${JAVA_OPTS} \
        -Dcatalina.base=${CATALINA_BASE} \
        -Dcatalina.home=${CATALINA_HOME} \
        -outfile ${CATALINA_BASE}/logs/catalina.out \
        -errfile ${CATALINA_BASE}/logs/catalina.err \
        -pidfile ${CATALINA_BASE}/pid \
        org.apache.catalina.startup.Bootstrap start
     popd
}

function stopTomcatAtLocation() { 
    if [ -z "$1" ]; then echo "stopTomcatAtLocation called without any arguments"; exit 1; fi
    export CATALINA_BASE=$1
    echo "Stopping tomcat at location ${CATALINA_BASE}"
    pushd ${CATALINA_BASE}/logs
    ${CATALINA_HOME}/bin/jsvc \
        -server \
        -cp ${CATALINA_HOME}/bin/bootstrap.jar:${CATALINA_HOME}/bin/tomcat-juli.jar \
        ${JAVA_OPTS} \
        -Dcatalina.base=${CATALINA_BASE} \
        -Dcatalina.home=${CATALINA_HOME} \
        -outfile ${CATALINA_BASE}/logs/catalina.out \
        -errfile ${CATALINA_BASE}/logs/catalina.err \
        -pidfile ${CATALINA_BASE}/pid \
        -stop \
        org.apache.catalina.startup.Bootstrap 
     popd
}


if [ "$1" = "stop" ]
then
    echo "Stopping all servers"
    stopTomcatAtLocation ${DEPLOY_DIR}/engine
    stopTomcatAtLocation ${DEPLOY_DIR}/retrieval
    stopTomcatAtLocation ${DEPLOY_DIR}/etl
    stopTomcatAtLocation ${DEPLOY_DIR}/mgmt
    exit
fi


echo "Appliance identity: $ARCHAPPL_MYIDENTITY"
echo "Appliance site: $ARCHAPPL_SITEID"
echo "STS: $ARCHAPPL_SHORT_TERM_FOLDER"
echo "MTS: $ARCHAPPL_MEDIUM_TERM_FOLDER"
echo "LTS: $ARCHAPPL_LONG_TERM_FOLDER"
echo "We are building for site ${ARCHAPPL_SITEID}"

# Enable core dumps in case the JVM fails
ulimit -c unlimited
export LD_LIBRARY_PATH=/scratch/Work/tomcat/latest/bin/tomcat-native-1.1.20-src/jni/native/.libs:${LD_LIBRARY_PATH}
export JAVA_OPTS="-XX:MaxPermSize=128M -Xmx1G -Dorg.apache.catalina.level=FINEST -ea"

stopTomcatAtLocation ${DEPLOY_DIR}/engine
stopTomcatAtLocation ${DEPLOY_DIR}/retrieval
stopTomcatAtLocation ${DEPLOY_DIR}/etl
stopTomcatAtLocation ${DEPLOY_DIR}/mgmt

# Clean the source folder of any deployed webapps
pushd $TOMCAT_HOME/webapps && rm -rf mgmt*; popd
pushd $TOMCAT_HOME/webapps && rm -rf engine*; popd
pushd $TOMCAT_HOME/webapps && rm -rf etl*; popd
pushd $TOMCAT_HOME/webapps && rm -rf retrieval*; popd


pushd ${PROJECT_DIR}; ant clean; ant; popd; 
pushd ${PROJECT_DIR}; ./deployMultipleTomcats.py ${DEPLOY_DIR}; if [[ $? != 0 ]]; then echo "Multiple deploy did not succeed; aborting"; exit 1; fi; popd

pushd ${DEPLOY_DIR}/mgmt/webapps && rm -rf mgmt*; cp ${PROJECT_DIR}/../mgmt.war .; popd; 
pushd ${DEPLOY_DIR}/engine/webapps && rm -rf engine*; cp ${PROJECT_DIR}/../engine.war .; popd; 
pushd ${DEPLOY_DIR}/etl/webapps && rm -rf etl*; cp ${PROJECT_DIR}/../etl.war .; popd; 
pushd ${DEPLOY_DIR}/retrieval/webapps && rm -rf retrieval*; cp ${PROJECT_DIR}/../retrieval.war .; popd; 

# We use jsvc to start the various webapps
# First check to see if this exists
if [[ ! -f ${CATALINA_HOME}/bin/jsvc ]]
then
    echo "Cannot find jsvc from commons-daemon in the ${CATALINA_HOME}/bin folder."
    exit 1
fi


startTomcatAtLocation ${DEPLOY_DIR}/mgmt
startTomcatAtLocation ${DEPLOY_DIR}/engine
startTomcatAtLocation ${DEPLOY_DIR}/etl
startTomcatAtLocation ${DEPLOY_DIR}/retrieval

tail -f ${DEPLOY_DIR}/mgmt/logs/catalina.out

