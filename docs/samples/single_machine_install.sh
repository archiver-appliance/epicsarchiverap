#!/bin/bash

echo "This script runs thru a typical install scenario for a single machine"
echo "You can use this to create a standard multi-instance (one Tomcat for ear WAR) tomcat deployment in a multi machine cluster by setting the ARCHAPPL_APPLIANCES and the ARCHAPPL_MYIDENTITY"
echo "For installations in a cluster, please do create a valid appliances.xml and export ARCHAPPL_APPLIANCES and ARCHAPPL_MYIDENTITY"


export SCRIPTS_DIR=`dirname $0`
if [[ ! -f ${SCRIPTS_DIR}/deployMultipleTomcats.py ]]
then
  echo "Unable to determine location of this script"
  exit 1
fi

if [[ ! -f ${SCRIPTS_DIR}/../mgmt.war ]]
then
  echo "We need to run the script in the extracted tar.gz folder. Cannot find the mgmt.war"
  exit 1
fi

if [[ -z ${JAVA_HOME} ]]
then
  echo "Please set JAVA_HOME to point to a 1.8 JDK"
  exit 1
fi

if [[ ! -f  ${JAVA_HOME}/include/linux/jni_md.h ]]
then
  echo "Missing the include/jni.md.h file in ${JAVA_HOME}. Please set JAVA_HOME to point to a 1.8 JDK (not a JRE)"
  exit 1
fi

export PATH=${JAVA_HOME}/bin:${PATH}

java -version 2>&1 | grep 'version "1.8'
if (( ( $? != 0 ) ))
then
  echo "Cannot find the string 1.8 in java -version. Please set JAVA_HOME to point to a 1.8 JDK"
  exit 1
fi


MSG="Pick a folder (preferably empty) where you'd like to create the Tomcat instances."
echo $MSG
export DEPLOY_DIR=$(zenity  --title "$MSG" --directory --file-selection)
echo "Setting DEPLOY_DIR to ${DEPLOY_DIR}"
if [[ ! -d ${DEPLOY_DIR} ]]
then
	MSG="${DEPLOY_DIR} does not seem to be a folder"
	echo ${MSG}
	zenity --text="${MSG}" --error
	exit 1
fi

MSG="Where's the Tomcat distribution (tar.gz)?"
echo $MSG
TOMCAT_DISTRIBUTION=$(zenity  --title "$MSG" --file-selection)

if [[ ! -f ${TOMCAT_DISTRIBUTION} ]]
then
	MSG="${TOMCAT_DISTRIBUTION} does not seem to be a valid file"
	echo ${MSG}
	zenity --text="${MSG}" --error
	exit 1
fi

tar -C ${DEPLOY_DIR} -zxf  ${TOMCAT_DISTRIBUTION}

pushd ${DEPLOY_DIR}
TOMCAT_VERSION_FOLDER=`ls -d apache-tomca* | head -1`
popd

export TOMCAT_HOME=${DEPLOY_DIR}/${TOMCAT_VERSION_FOLDER}
echo "Setting TOMCAT_HOME to ${TOMCAT_HOME}"

if [[ ! -f ${TOMCAT_HOME}/conf/server.xml ]]
then
	MSG="${TOMCAT_HOME} is missing the conf/server.xml file. Was ${TOMCAT_DISTRIBUTION} a valid Tomcat tar.gz?"
	echo ${MSG}
	zenity --text="${MSG}" --error
	exit 1
fi

# Copy over the mysql client jar and create a logj.properties file.
cat > ${TOMCAT_HOME}/lib/log4j.properties <<EOF
# Set root logger level and its only appender to A1.
log4j.rootLogger=ERROR, A1
log4j.logger.config.org.epics.archiverappliance=INFO
log4j.logger.org.apache.http=ERROR


# A1 is set to be a DailyRollingFileAppender
log4j.appender.A1=org.apache.log4j.DailyRollingFileAppender
log4j.appender.A1.File=arch.log
log4j.appender.A1.DatePattern='.'yyyy-MM-dd


# A1 uses PatternLayout.
log4j.appender.A1.layout=org.apache.log4j.PatternLayout
log4j.appender.A1.layout.ConversionPattern=%-4r [%t] %-5p %c %x - %m%n
EOF

# Build the Apache Commons Daemon that ships with Tomcat
pushd ${TOMCAT_HOME}/bin
tar zxf commons-daemon-native.tar.gz
COMMONS_DAEMON_VERSION_FOLDER=`ls -d commons-daemon-*-native-src | head -1`
popd

pushd ${TOMCAT_HOME}/bin/${COMMONS_DAEMON_VERSION_FOLDER}/unix
./configure
make
if [[ ! -f jsvc ]]
then
	MSG="Cannot seem to build Apache Commons demon in ${TOMCAT_HOME}/bin/${COMMONS_DAEMON_VERSION_FOLDER}/unix"
	echo ${MSG}
	zenity --text="${MSG}" --error
	exit 1
fi
popd

cp ${TOMCAT_HOME}/bin/${COMMONS_DAEMON_VERSION_FOLDER}/unix/jsvc ${TOMCAT_HOME}/bin


MSG="Where's the mysql client jar? - this is named something like mysql-connector-java-5.1.21-bin.jar."
echo $MSG
MYSQL_CLIENT_JAR=$(zenity  --title "$MSG" --file-selection)

if [[ ! -f ${MYSQL_CLIENT_JAR} ]]
then
	MSG="${MYSQL_CLIENT_JAR} does not seem to be a valid file"
	echo ${MSG}
	zenity --text="${MSG}" --error
	exit 1
fi

cp ${MYSQL_CLIENT_JAR} ${TOMCAT_HOME}/lib
echo "Done copying the mysql client library to ${TOMCAT_HOME}/lib"

if [[ -z ${ARCHAPPL_APPLIANCES} ]]
then
	MSG="I see you have not defined the ARCHAPPL_APPLIANCES environment variable. If we proceed, I'll automatically generate one in ${DEPLOY_DIR}. Should we proceed?"
	echo ${MSG}
	zenity --text="${MSG}" --question
	if [[ $? == 0 ]] ; then
		FQ_HOSTNAME=`hostname -f`
# Create an appliances.xml file and set up this appliance's identity.
		cat > ${DEPLOY_DIR}/appliances.xml <<EOF
 <appliances>
   <appliance>
     <identity>appliance0</identity>
     <cluster_inetport>${FQ_HOSTNAME}:16670</cluster_inetport>
     <mgmt_url>http://${FQ_HOSTNAME}:17665/mgmt/bpl</mgmt_url>
     <engine_url>http://${FQ_HOSTNAME}:17666/engine/bpl</engine_url>
     <etl_url>http://${FQ_HOSTNAME}:17667/etl/bpl</etl_url>
     <retrieval_url>http://localhost:17668/retrieval/bpl</retrieval_url>
     <data_retrieval_url>http://${FQ_HOSTNAME}:17668/retrieval</data_retrieval_url>
   </appliance>
 </appliances>
EOF

		export ARCHAPPL_APPLIANCES=${DEPLOY_DIR}/appliances.xml
		export ARCHAPPL_MYIDENTITY=appliance0
	else
		MSG="Please set your ARCHAPPL_APPLIANCES and ARCHAPPL_MYIDENTITY and rerun this script"
		echo ${MSG}
		zenity --text="${MSG}" --info
		exit 1
	fi
else
	if [[ -z ${ARCHAPPL_MYIDENTITY} ]]
	then
		MSG="ARCHAPPL_APPLIANCES was defined but ARCHAPPL_MYIDENTITY was not defined. We need both of these to be defined."
		echo ${MSG}
		zenity --text="${MSG}" --error
		exit 1
	fi
fi

if [[ ! -f ${ARCHAPPL_APPLIANCES} ]]
then
	MSG="${ARCHAPPL_APPLIANCES} does not seem to be a valid file."
	echo ${MSG}
	zenity --text="${MSG}" --error
	exit 1
fi

echo "Calling ${SCRIPTS_DIR}/deployMultipleTomcats.py ${DEPLOY_DIR}"
${SCRIPTS_DIR}/deployMultipleTomcats.py ${DEPLOY_DIR}

if [[ ! -d ${DEPLOY_DIR}/mgmt/webapps ]]
then
	MSG="After calling deployMultipleTomcats.py to create the tomcats for the components, we did not find the mgmt ui. One reason for this is a mismatch between the appliance identity ${ARCHAPPL_MYIDENTITY} and your appliances file at ${ARCHAPPL_APPLIANCES}. Please make that the appliance information for ${ARCHAPPL_MYIDENTITY} in ${ARCHAPPL_APPLIANCES} is correct."
	echo ${MSG}
	zenity --text="${MSG}" --error
	exit 1	
fi



if [[ -z `which mysql` ]]
then
	MSG="Unable to execute the mysql client. Is it in the PATH?"
	echo ${MSG}
	zenity --text="${MSG}" --error
	exit 1
fi

MSG="Please enter a MySQL Connection string to an existing database like so"
DEFAULT_MYSQL_CONNECTION_STRING="--user=archappl --password=archappl --database=archappl"
echo $MSG
export MYSQL_CONNECTION_STRING=$(zenity --entry --width=800 --text="$MSG" --entry-text="${DEFAULT_MYSQL_CONNECTION_STRING}")
echo "Setting MYSQL_CONNECTION_STRING to ${MYSQL_CONNECTION_STRING}"

# Use a SHOW DATABASES command to see if the connection string is valid
let numtries=5
mysql ${MYSQL_CONNECTION_STRING} -e "SHOW DATABASES" | grep information_schema 
while (( $? && ( ${numtries} > 1) ))
do
	let numtries=numtries-1
	MSG="MySQL connection string ${MYSQL_CONNECTION_STRING} does not seem to be a valid connection string"
	echo ${MSG}
	zenity --text="${MSG}" --error
	if (( ( ${numtries} <= 1 )  ))
	then
		# We've tried a few times; must be a bug in the script.
		exit 1
	fi
	
	MSG="Please enter a MySQL Connection string like so"
	DEFAULT_MYSQL_CONNECTION_STRING = ${MYSQL_CONNECTION_STRING}
	echo $MSG
	export MYSQL_CONNECTION_STRING=$(zenity --entry --width=800 --text="$MSG" --entry-text="${DEFAULT_MYSQL_CONNECTION_STRING}")
	echo "Setting MYSQL_CONNECTION_STRING to ${MYSQL_CONNECTION_STRING}"
	
	mysql ${MYSQL_CONNECTION_STRING} -e "SHOW DATABASES" | grep information_schema
done

# If we are here, the MYSQL_CONNECTION_STRING is valid.
# Let's check to see if the tables exist.
mysql ${MYSQL_CONNECTION_STRING} -e "SHOW TABLES" | grep PVTypeInfo
if (( ( $? != 0 ) ))
then
	MSG="I do not see the PVTypeInfo table in ${MYSQL_CONNECTION_STRING}? Should we go ahead and create the tables? This step will delete any old data that you have."
	echo ${MSG}
	zenity --text="${MSG}" --question
	if [[ $? == 0 ]] ; then
		echo "Creating tables in ${MYSQL_CONNECTION_STRING}"
		mysql ${MYSQL_CONNECTION_STRING} < ${SCRIPTS_DIR}/archappl_mysql.sql
		
		mysql ${MYSQL_CONNECTION_STRING} -e "SHOW TABLES" | grep PVTypeInfo
		if (( ( $? != 0 ) ))
		then
			MSG="Cannot create the MySQL tables. Do you have the right permissions?"
			echo ${MSG}
			zenity --text="${MSG}" --error
			exit 1
		fi
	else
		echo "Skipping creating MySQL tables."
	fi
else 
	MSG="The EPICS archiver appliance tables already exist in the schema accessed by using ${MYSQL_CONNECTION_STRING}"
	echo ${MSG}
	zenity --text="${MSG}" --info	
fi


# Temporarily set TOMCAT_HOME to the mgmt webapp.
TOMCAT_HOME=${DEPLOY_DIR}/mgmt
echo "Setting TOMCAT_HOME to the mgmt webapp in ${TOMCAT_HOME}"

# Add the connection pool to the context.xml file
${SCRIPTS_DIR}/addMysqlConnPool.py

# Restore TOMCAT_HOME
TOMCAT_HOME=${DEPLOY_DIR}/${TOMCAT_VERSION_FOLDER}
echo "Setting TOMCAT_HOME to ${TOMCAT_HOME}"

# Generate the deployRelease.sh script
cat > ${DEPLOY_DIR}/deployRelease.sh <<EOF
#!/bin/bash

# This script deploys a new build onto the EPICS archiver appliance installation at ${DEPLOY_DIR}
# Call this script with the folder that contains the expanded tar.gz; that is, the folder that contains the various WAR files

export JAVA_HOME=${JAVA_HOME}
export PATH=\${JAVA_HOME}/bin:\${PATH}
export TOMCAT_HOME=${TOMCAT_HOME}
export CATALINA_HOME=${TOMCAT_HOME}
export DEPLOY_DIR=${DEPLOY_DIR}

if [[ \$# -eq 0 ]]
then
	echo "You need to call deployRelease.sh with the folder containing the mgmt and other war files."
	exit 1
fi

WARSRC_DIR=\${1}

if [[ ! -f \${WARSRC_DIR}/mgmt.war ]]
then
	echo "You need to call deployRelease.sh with the folder containing the mgmt and other war files. The folder \${WARSRC_DIR} does not seem to have a mgmt.war."
	exit 1
fi

echo "Deploying a new release from \${WARSRC_DIR} onto \${DEPLOY_DIR}"
pushd \${DEPLOY_DIR}/mgmt/webapps && rm -rf mgmt*; cp \${WARSRC_DIR}/mgmt.war .; mkdir mgmt; cd mgmt; jar xf ../mgmt.war; popd; 
pushd \${DEPLOY_DIR}/engine/webapps && rm -rf engine*; cp \${WARSRC_DIR}/engine.war .; mkdir engine; cd engine; jar xf ../engine.war; popd; 
pushd \${DEPLOY_DIR}/etl/webapps && rm -rf etl*; cp \${WARSRC_DIR}/etl.war .; mkdir etl; cd etl; jar xf ../etl.war; popd; 
pushd \${DEPLOY_DIR}/retrieval/webapps && rm -rf retrieval*; cp \${WARSRC_DIR}/retrieval.war .; mkdir retrieval; cd retrieval; jar xf ../retrieval.war; popd;
echo "Done deploying a new release from \${WARSRC_DIR} onto \${DEPLOY_DIR}"

# Post installation steps for changing look and feel etc.
if [[ -f \${WARSRC_DIR}/site_specific_content/template_changes.html ]]
then
  echo "Modifying static content to cater to site specific information"
  java -cp \${DEPLOY_DIR}/mgmt/webapps/mgmt/WEB-INF/classes org.epics.archiverappliance.mgmt.bpl.SyncStaticContentHeadersFooters \${WARSRC_DIR}/site_specific_content/template_changes.html \${DEPLOY_DIR}/mgmt/webapps/mgmt/ui
fi

if [[ -d \${WARSRC_DIR}/site_specific_content/img ]]
then
  echo "Replacing site specific images"
  cp -R \${WARSRC_DIR}/site_specific_content/img/* \${DEPLOY_DIR}/mgmt/webapps/mgmt/ui/comm/img/
fi


EOF

chmod +x ${DEPLOY_DIR}/deployRelease.sh

# Call deployRelease to deploy the WAR files.
WARSRC_DIR=`python -c "import os; print os.path.abspath('${SCRIPTS_DIR}/..')"`
echo "Calling deploy release with ${DEPLOY_DIR}/deployRelease.sh ${WARSRC_DIR}"
${DEPLOY_DIR}/deployRelease.sh ${WARSRC_DIR}

if [[ ! -f ${DEPLOY_DIR}/mgmt/webapps/mgmt/ui/index.html ]]
then
	MSG="After deploying the release, cannot find a required file. The deployment did not succeed."
	echo ${MSG}
	zenity --text="${MSG}" --error
	exit 1	
fi

cat ${SCRIPTS_DIR}/sampleStartup.sh \
	| sed -e "s;export JAVA_HOME=/opt/local/java/latest;export JAVA_HOME=${JAVA_HOME};g" \
	| sed -e "s;export TOMCAT_HOME=/opt/local/tomcat;export TOMCAT_HOME=${TOMCAT_HOME};g" \
	| sed -e "s;export ARCHAPPL_DEPLOY_DIR=/opt/local/archappl;export ARCHAPPL_DEPLOY_DIR=${DEPLOY_DIR};g" \
	| sed -e "s;export ARCHAPPL_APPLIANCES=/nfs/archiver/appliances.xml;export ARCHAPPL_APPLIANCES=${ARCHAPPL_APPLIANCES};g" \
	| sed -e "s;export ARCHAPPL_MYIDENTITY=\"appliance0\";export ARCHAPPL_MYIDENTITY=\"${ARCHAPPL_MYIDENTITY}\";g" \
	> ${DEPLOY_DIR}/sampleStartup.sh
	
MSG="Do you have a site specific policies.py file?"
echo ${MSG}
zenity --text="${MSG}" --question
if [[ $? == 0 ]]
then
	MSG="Where's your site specific policies.py file?"
	echo $MSG
	SITE_SPECIFIC_POLICIES_FILE=$(zenity  --title "$MSG" --file-selection)

	if [[ ! -f ${SITE_SPECIFIC_POLICIES_FILE} ]]
	then
		MSG="${SITE_SPECIFIC_POLICIES_FILE} does not seem to be a valid file"
		echo ${MSG}
		zenity --text="${MSG}" --error
		exit 1
	fi

	echo "Setting ARCHAPPL_POLICIES to ${SITE_SPECIFIC_POLICIES_FILE}"
	cat ${DEPLOY_DIR}/sampleStartup.sh \
	| sed -e "s;# export ARCHAPPL_POLICIES=/nfs/epics/archiver/production_policies.py;export ARCHAPPL_POLICIES=${SITE_SPECIFIC_POLICIES_FILE};g" \
	> ${DEPLOY_DIR}/sampleStartup.sh.withpolicies
	
	mv -f ${DEPLOY_DIR}/sampleStartup.sh.withpolicies ${DEPLOY_DIR}/sampleStartup.sh
fi
	
chmod +x ${DEPLOY_DIR}/sampleStartup.sh

MSG="Done with the installation. Please use ${DEPLOY_DIR}/sampleStartup.sh to start and stop the appliance and ${DEPLOY_DIR}/deployRelease.sh to deploy a new release." 
echo ${MSG}
zenity --text="${MSG}" --info	



