#!/bin/bash

# Sample startup script for the archiver appliance
# Please change the various environment variables to suit your environment.

# We assume that we inherit the EPICS environment variables from something that calls this script
# However, if this is a init.d startup script, this is not going to be the case and we'll need to add them here.
# This includes setting up the LD_LIBRARY_PATH to include the JCA .so file.
source /opt/local/setEPICSEnv.sh

export JAVA_HOME=/opt/local/java/latest
export PATH=${JAVA_HOME}/bin:${PATH}
# We use a lot of memory; so be generous with the heap. 
export JAVA_OPTS="-XX:MaxPermSize=128M -XX:+UseG1GC -Xmx4G -Xms4G -ea"

# Set up Tomcat home
export TOMCAT_HOME=/opt/local/tomcat

# Set up the root folder of the individual Tomcat instances.
export ARCHAPPL_DEPLOY_DIR=/opt/local/archappl

# Set appliance.xml and the identity of this appliance
export ARCHAPPL_APPLIANCES=/nfs/archiver/appliances.xml
export ARCHAPPL_MYIDENTITY="appliance0"

# If you have your own policies file, please change this line.
# export ARCHAPPL_POLICIES=/nfs/epics/archiver/production_policies.py

# Set the location of short term and long term stores; this is necessary only if your policy demands it
export ARCHAPPL_SHORT_TERM_FOLDER=/arch/sts/ArchiverStore
export ARCHAPPL_MEDIUM_TERM_FOLDER=/arch/mts/ArchiverStore
export ARCHAPPL_LONG_TERM_FOLDER=/arch/lts/ArchiverStore

if [[ ! -d ${TOMCAT_HOME} ]]
then
    echo "Unable to determine the source of the tomcat distribution"
    exit 1
fi

if [[ ! -f ${ARCHAPPL_APPLIANCES} ]]
then
    echo "Unable to find appliances.xml at ${ARCHAPPL_APPLIANCES}"
    exit 1
fi

# Enable core dumps in case the JVM fails
ulimit -c unlimited




function startTomcatAtLocation() { 
    if [ -z "$1" ]; then echo "startTomcatAtLocation called without any arguments"; exit 1; fi
    export CATALINA_HOME=$TOMCAT_HOME
    export CATALINA_BASE=$1
    echo "Starting tomcat at location ${CATALINA_BASE}"
    
    ARCH=`uname -m`
    if [[ $ARCH == 'x86_64' || $ARCH == 'amd64' ]]
    then
      echo "Using 64 bit versions of libraries"
      export LD_LIBRARY_PATH=${CATALINA_BASE}/webapps/mgmt/WEB-INF/lib/native/linux-x86_64:${LD_LIBRARY_PATH}
    else
      echo "Using 32 bit versions of libraries"
      export LD_LIBRARY_PATH=${CATALINA_BASE}/webapps/mgmt/WEB-INF/lib/native/linux-x86:${LD_LIBRARY_PATH}
    fi
    
    pushd ${CATALINA_BASE}/logs
    ${CATALINA_HOME}/bin/jsvc \
        -server \
        -cp ${CATALINA_HOME}/bin/bootstrap.jar:${CATALINA_HOME}/bin/tomcat-juli.jar \
        ${JAVA_OPTS} \
        -Dcatalina.base=${CATALINA_BASE} \
        -Dcatalina.home=${CATALINA_HOME} \
        -cwd ${CATALINA_BASE}/logs \
        -outfile ${CATALINA_BASE}/logs/catalina.out \
        -errfile ${CATALINA_BASE}/logs/catalina.err \
        -pidfile ${CATALINA_BASE}/pid \
        org.apache.catalina.startup.Bootstrap start
     popd
}

function stopTomcatAtLocation() { 
    if [ -z "$1" ]; then echo "stopTomcatAtLocation called without any arguments"; exit 1; fi
    export CATALINA_HOME=$TOMCAT_HOME
    export CATALINA_BASE=$1
    echo "Stopping tomcat at location ${CATALINA_BASE}"
    pushd ${CATALINA_BASE}/logs
    ${CATALINA_HOME}/bin/jsvc \
        -server \
        -cp ${CATALINA_HOME}/bin/bootstrap.jar:${CATALINA_HOME}/bin/tomcat-juli.jar \
        ${JAVA_OPTS} \
        -Dcatalina.base=${CATALINA_BASE} \
        -Dcatalina.home=${CATALINA_HOME} \
        -cwd ${CATALINA_BASE}/logs \
        -outfile ${CATALINA_BASE}/logs/catalina.out \
        -errfile ${CATALINA_BASE}/logs/catalina.err \
        -pidfile ${CATALINA_BASE}/pid \
        -stop \
        org.apache.catalina.startup.Bootstrap 
     popd
}

function stop() { 
	stopTomcatAtLocation ${ARCHAPPL_DEPLOY_DIR}/engine
	stopTomcatAtLocation ${ARCHAPPL_DEPLOY_DIR}/retrieval
	stopTomcatAtLocation ${ARCHAPPL_DEPLOY_DIR}/etl
	stopTomcatAtLocation ${ARCHAPPL_DEPLOY_DIR}/mgmt
}

function start() { 
	startTomcatAtLocation ${ARCHAPPL_DEPLOY_DIR}/mgmt
	startTomcatAtLocation ${ARCHAPPL_DEPLOY_DIR}/engine
	startTomcatAtLocation ${ARCHAPPL_DEPLOY_DIR}/etl
	startTomcatAtLocation ${ARCHAPPL_DEPLOY_DIR}/retrieval
}


# See how we were called.
case "$1" in
  start)
	start
	;;
  stop)
	stop
	;;
  restart)
	stop
	start
	;;
  *)
	echo $"Usage: $0 {start|stop|restart}"
	exit 2
esac


