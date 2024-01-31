#!/bin/sh
set -e
# This script takes as arguments
# 1) A tomcat distribution.
# 
# It then unpacks the tomcat distribution in the current folder and deploys the four WAR files onto this tomcat instance
# The server.xml is overwritten with a default server.xml
# A log4j2.xml with INFO logging is copied into the tomcat lib folder.
# Finally, the tomcat instance is started using catalina run
#
# If all goes well, we should be able to point the browser to tomcat instance and then see the archiver homescreen.
#
# This needs to be a UNIX file.

DEFAULT_LOG_LEVEL=ERROR
START_IN_BACKGROUND=FALSE

die() {
  echo "$1" 2>&1
  exit 1
}

ensuredir() {
  [ "$1" ] || die "Empty directory name"
  [ -d "$1" ] || install -d "$1"
}

# find the directory containing this script
SRCDIR="$(dirname "$(readlink -f "$0")")"
[ -r "$SRCDIR/mgmt.war" ] || die "Incorrect SRCDIR=$SRCDIR"
DATADIR="$PWD"

while getopts ":vsd:" opt; do
  case $opt in
    v)
      echo "Setting log levels to DEBUG." >&2
      DEFAULT_LOG_LEVEL=DEBUG
      ;;
    s)
      echo "Starting in the background" >&2
      START_IN_BACKGROUND=TRUE
      ;;
    d)
      DATADIR="$OPTARG"
      ;;
    \?)
      echo "Invalid option: -$OPTARG" >&2
      exit 1
      ;;
  esac
done

shift $(($OPTIND - 1))

[ $# -lt 1 ] && die "Usage: $0 <Tomcat Distribution>"

TOMCAT_DISTRIBUTION=$1

[ -f ${TOMCAT_DISTRIBUTION} ] || die "${TOMCAT_DISTRIBUTION} is not a valid file?"

FQ_HOSTNAME=`hostname -f`

if [ -z "${FQ_HOSTNAME}" ]
then
  echo "Unknown/empty hostname. Setting hostname to localhost"
  FQ_HOSTNAME="localhost"
fi


[ -z "`type -p java`" -o -z "`type -p jar`" ] && die "Cannot find the java/jar executables. Please set your PATH correctly."


ensuredir "$DATADIR"

cd "$DATADIR"
DATADIR="$PWD"

echo "SRCDIR=$SRCDIR"
echo "DATADIR=$DATADIR"
 
export ARCHAPPL_APPLIANCES="${DATADIR}/appliances.xml"
export ARCHAPPL_MYIDENTITY=appliance0

# Create an appliances.xml file and set up this appliance's identity.
cat > "${ARCHAPPL_APPLIANCES}" <<EOF
 <appliances>
   <appliance>
     <identity>appliance0</identity>
     <cluster_inetport>localhost:16670</cluster_inetport>
     <mgmt_url>http://localhost:17665/mgmt/bpl</mgmt_url>
     <engine_url>http://localhost:17665/engine/bpl</engine_url>
     <etl_url>http://localhost:17665/etl/bpl</etl_url>
     <retrieval_url>http://localhost:17665/retrieval/bpl</retrieval_url>
     <data_retrieval_url>http://${FQ_HOSTNAME}:17665/retrieval</data_retrieval_url>
   </appliance>
 </appliances>
EOF

if [ ! -f "${ARCHAPPL_APPLIANCES}" ]
then
	echo "We just generated the appliances.xml file but we cannot seem to find it here ${ARCHAPPL_APPLIANCES}"
	echo "The archiver appliance will not start successfully without this file"
	exit 1
fi


# Set up the storage folders based on the default policy..
# Use ramdisk as the STS unless this has been already set in the environment.
if [ -z ${ARCHAPPL_SHORT_TERM_FOLDER} ] 
then
	export ARCHAPPL_SHORT_TERM_FOLDER=/dev/shm/quickstart_sts
fi

if [ -z ${ARCHAPPL_MEDIUM_TERM_FOLDER} ] 
then
  export ARCHAPPL_MEDIUM_TERM_FOLDER="${DATADIR}/MTS"
  ensuredir "$ARCHAPPL_MEDIUM_TERM_FOLDER"
fi

if [ -z ${ARCHAPPL_LONG_TERM_FOLDER} ] 
then
  export ARCHAPPL_LONG_TERM_FOLDER="${DATADIR}/LTS"
  ensuredir "$ARCHAPPL_LONG_TERM_FOLDER"
fi
# Use an in memory persistence layer, if one is not defined in the environment
if [ -z ${ARCHAPPL_PERSISTENCE_LAYER} ] 
then
	export ARCHAPPL_PERSISTENCE_LAYER=org.epics.archiverappliance.config.persistence.InMemoryPersistence
fi

echo "Using ${ARCHAPPL_PERSISTENCE_LAYER} as the persistence layer"

# Tell the appliance that we are deploying all the components in one VM.
# This reduces the thread count and other parameters in an effort to optimize memory.
export ARCHAPPL_ALL_APPS_ON_ONE_JVM="true"

if [ -d quickstart_tomcat ]
then
    echo "Found an older quickstart_tomcat folder. Stopping any existing instances."
    cd quickstart_tomcat
    for tomcatfolder in apache-tomcat*; do ${tomcatfolder}/bin/catalina.sh stop; done
    sleep 30
    cd "$DATADIR"
    echo "Deleting an older quickstart_tomcat"
    rm -rf quickstart_tomcat
fi

mkdir quickstart_tomcat
tar -C quickstart_tomcat -zxf  "${TOMCAT_DISTRIBUTION}"
cd quickstart_tomcat
# eg. "apache-tomcat-9.0.20"
TOMCAT_VERSION_FOLDER=`ls -d apache-tomca* | head -1`

# Make sure we have a valid server.xml
if [ ! -f "${TOMCAT_VERSION_FOLDER}/conf/server.xml" ]
then
  echo "After expanding the tomcat distribution, ${TOMCAT_DISTRIBUTION} into quickstart_tomcat/${TOMCAT_VERSION_FOLDER}, we cannot find ${TOMCAT_VERSION_FOLDER}/server.xml as expected."
  exit 1
fi

# Write a minimal server.xml into unpacked tomcat distribution.
cat > "${TOMCAT_VERSION_FOLDER}/conf/server.xml" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<Server port="8005" shutdown="SHUTDOWN">
  <Listener className="org.apache.catalina.startup.VersionLoggerListener" />
  <Listener className="org.apache.catalina.core.JreMemoryLeakPreventionListener" />
  <Listener className="org.apache.catalina.mbeans.GlobalResourcesLifecycleListener" />
  <Listener className="org.apache.catalina.core.ThreadLocalLeakPreventionListener" />

  <Service name="Catalina">
    <Connector port="17665" protocol="HTTP/1.1" connectionTimeout="20000" redirectPort="8443" />
    <Engine name="Catalina" defaultHost="localhost">
      <Host name="localhost"  appBase="webapps" unpackWARs="true" autoDeploy="true">
        <Valve className="org.apache.catalina.valves.AccessLogValve" directory="logs" prefix="localhost_access_log" suffix=".txt" pattern="%h %l %u %t &quot;%r&quot; %s %b" />
      </Host>
    </Engine>
  </Service>
</Server>
EOF

# Write a log4.properties file into the lib folder
cat > "${TOMCAT_VERSION_FOLDER}/lib/log4j2.xml" <<EOF
<Configuration>
   <Appenders>
       <Console name="STDOUT" target="SYSTEM_OUT">
           <PatternLayout pattern="%d %-5p [%t] %C{2} (%F:%L) - %m%n"/>
       </Console>
   </Appenders>
   <Loggers>
       <Logger name="org.apache.log4j.xml" level="${DEFAULT_LOG_LEVEL}"/>
       <Root level="${DEFAULT_LOG_LEVEL}">
           <AppenderRef ref="STDOUT"/>
       </Root>
   </Loggers>
</Configuration>
EOF

# Now, deploy the WAR files. 
cp "$SRCDIR/mgmt.war" ${TOMCAT_VERSION_FOLDER}/webapps
cp "$SRCDIR/engine.war" ${TOMCAT_VERSION_FOLDER}/webapps
cp "$SRCDIR/etl.war" ${TOMCAT_VERSION_FOLDER}/webapps
cp "$SRCDIR/retrieval.war" ${TOMCAT_VERSION_FOLDER}/webapps

# Unpack the mgmt war so that we can replace any images etc..
cd "${TOMCAT_VERSION_FOLDER}/webapps"

[ -d mgmt ] && rm -rf mgmt
mkdir mgmt
cd mgmt
jar xf "$SRCDIR/mgmt.war"

cd "$DATADIR/quickstart_tomcat"

pwd
ls -ltr 

if [ -f "$SRCDIR/site_specific_content/template_changes.html" ]
then
  echo "Modifying static content to cater to site specific information"
java org.epics.archiverappliance.mgmt.bpl.SyncStaticContentHeadersFooters \
   "$SRCDIR/site_specific_content/template_changes.html" \
   ${TOMCAT_VERSION_FOLDER}/webapps/mgmt/ui
fi

if [ -d "$SRCDIR/site_specific_content/img" ]
then
  echo "Replacing site specific images"
  cp -R "$SRCDIR/site_specific_content/img"/* ${TOMCAT_VERSION_FOLDER}/webapps/mgmt/ui/comm/img/
fi


# Start up the JVM with 1Gb of heap...
if [ -z ${JAVA_OPTS} ] 
then
   export JAVA_OPTS="-Xmx1G -ea"
fi

ARCH=`uname -m`
if [ "$ARCH" = "x86_64" ] || [ "$ARCH" = "amd64" ]
then
  echo "Using 64 bit versions of libraries"
  export LD_LIBRARY_PATH=${TOMCAT_VERSION_FOLDER}/webapps/engine/WEB-INF/lib/native/linux-x86_64:${LD_LIBRARY_PATH}
else
  echo "Using 32 bit versions of libraries"
  export LD_LIBRARY_PATH=${TOMCAT_VERSION_FOLDER}/webapps/engine/WEB-INF/lib/native/linux-x86:${LD_LIBRARY_PATH}
fi

# Start tomcat up...
if [ "$START_IN_BACKGROUND" = "TRUE" ]
then
  echo "Starting in the background..." 
  ${TOMCAT_VERSION_FOLDER}/bin/catalina.sh start
  sleep 5
else
  echo "Starting in the foreground..." 
  ${TOMCAT_VERSION_FOLDER}/bin/catalina.sh run
fi
