# Deploying the archiver appliance

## Create individual Tomcat containers for each web app

The `mgmt.war` file contains a script `deployMultipleTomcats.py` in the
`install` folder that will use the information in the `appliances.xml`
file and the identity of this appliance to generate individual Tomcat
containers from a single Tomcat install (identified by the environment
variable `TOMCAT_HOME`). To run this script, set the following
environment variables

1. `TOMCAT_HOME` - This is the Tomcat installation that you prepared in
   the previous steps.
2. `ARCHAPPL_APPLIANCES` - This points to the `appliances.xml` that you
   created in the previous steps.
3. `ARCHAPPL_MYIDENTITY` - This is the identity of the current
   appliance, for example `appliance0`. If this is not set, the system
   will default to using the machine's hostname as determined by
   making a call to
   `InetAddress.getLocalHost().getCanonicalHostName()`. However, this
   makes `ARCHAPPL_MYIDENTITY` a physical entity and not a logical
   entity; so, if you can, use a logical name for this entry. Note,
   this must match the `identity` element of this appliance as it is
   defined in the `appliances.xml`.

and then run the `deployMultipleTomcats.py` script passing in one
argument that identifies the parent folder of the individual Tomcat
containers.

```bash
$ export TOMCAT_HOME=/arch/single_machine_install/tomcats/apache-tomcat-11.0.12
$ export ARCHAPPL_APPLIANCES=/arch/single_machine_install/sample_appliances.xml
$ export ARCHAPPL_MYIDENTITY=appliance0
$ ./install_scripts/deployMultipleTomcats.py /arch/single_machine_install/tomcats
Using
    tomcat installation at /arch/single_machine_install/tomcats/apache-tomcat-11.0.12
    to generate deployments for appliance appliance0
    using configuration info from /arch/single_machine_install/sample_appliances.xml
    into folder /arch/single_machine_install/tomcats
The start/stop port is the standard Tomcat start/stop port. Changing it to something else random - 16000
The stop/start ports for the new instance will being at  16001
Generating tomcat folder for  mgmt  in location /arch/single_machine_install/tomcats/mgmt
Commenting connector with protocol  AJP/1.3 . If you do need this connector, you should un-comment this.
Generating tomcat folder for  engine  in location /arch/single_machine_install/tomcats/engine
Commenting connector with protocol  AJP/1.3 . If you do need this connector, you should un-comment this.
Generating tomcat folder for  etl  in location /arch/single_machine_install/tomcats/etl
Commenting connector with protocol  AJP/1.3 . If you do need this connector, you should un-comment this.
Generating tomcat folder for  retrieval  in location /arch/single_machine_install/tomcats/retrieval
Commenting connector with protocol  AJP/1.3 . If you do need this connector, you should un-comment this.
```

## Deploy the WAR files onto their respective containers

Deploying/upgrading a WAR file in a Tomcat container is very easy. Each
container has a `webapps` folder; all we have to do is to copy the
(newer) WAR into this folder and Tomcat will expand the WAR file and
deploy it on startup. The deployment/upgrade steps are

1. Stop all four Tomcat containers.
2. Remove the older WAR file and expanded WAR file from the `webapps`
   folder (if present).
3. Copy the newer WAR file into the `webapps` folder.
4. Optionally expand the WAR file after copying it over to the
   `webapps` folder
   - This lets you replace individual files in the expanded WAR file
     (for example, images, policies etc) giving you one more way to
     do site specific deployments.
5. Start all four Tomcat containers.

If `DEPLOY_DIR` is the parent folder of the individual Tomcat containers
and `WARSRC_DIR` is the location where the WAR files are present, then
the deploy steps (steps 2 and 3 in the list above) look something like

```bash
pushd ${DEPLOY_DIR}/mgmt/webapps && rm -rf mgmt*; cp ${WARSRC_DIR}/mgmt.war .; mkdir mgmt; cd mgmt; jar xf ../mgmt.war; popd;
pushd ${DEPLOY_DIR}/engine/webapps && rm -rf engine*; cp ${WARSRC_DIR}/engine.war .; mkdir engine; cd engine; jar xf ../engine.war; popd;
pushd ${DEPLOY_DIR}/etl/webapps && rm -rf etl*; cp ${WARSRC_DIR}/etl.war .; mkdir etl; cd etl; jar xf ../etl.war; popd;
pushd ${DEPLOY_DIR}/retrieval/webapps && rm -rf retrieval*; cp ${WARSRC_DIR}/retrieval.war .; mkdir retrieval; cd retrieval; jar xf ../retrieval.war; popd;
```

It is possible to deploy the 4 WAR files on other servlet containers or
to use other industry standard provisioning software. The details here
are guidelines for Tomcat. If you generate scripts for industry standard
provisioning software and are willing to share them, please add them to
the repository and contact the collaboration.

## Stopping and starting the individual Tomcats

Running multiple Tomcats on a single machine using the same install
requires two environment variables

1. `CATALINA_HOME` - This is the install folder for Tomcat that is
   common to all Tomcat instances; in our case this is `$TOMCAT_HOME`
2. `CATALINA_BASE` - This is the deploy folder for Tomcat that is
   specific to each Tomcat instance; in our case this is
   - `${DEPLOY_DIR}/mgmt`
   - `${DEPLOY_DIR}/etl`
   - `${DEPLOY_DIR}/engine`
   - `${DEPLOY_DIR}/retrieval`

If you are using Apache Commons Daemon, then two bash functions for
stopping and starting a Tomcat instance look something like

```bash
function startTomcatAtLocation() {
    if [ -z "$1" ]; then echo "startTomcatAtLocation called without any arguments"; exit 1; fi
    export CATALINA_HOME=${TOMCAT_HOME}
    export CATALINA_BASE=$1
    echo "Starting tomcat at location ${CATALINA_BASE}"
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
    export CATALINA_HOME=${TOMCAT_HOME}
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
```

and you'd invoke these using something like

```bash
stopTomcatAtLocation ${DEPLOY_DIR}/engine
stopTomcatAtLocation ${DEPLOY_DIR}/retrieval
stopTomcatAtLocation ${DEPLOY_DIR}/etl
stopTomcatAtLocation ${DEPLOY_DIR}/mgmt
```

and

```bash
startTomcatAtLocation ${DEPLOY_DIR}/mgmt
startTomcatAtLocation ${DEPLOY_DIR}/engine
startTomcatAtLocation ${DEPLOY_DIR}/etl
startTomcatAtLocation ${DEPLOY_DIR}/retrieval
```

Remember to set all the appropriate environment variables:

1. `JAVA_HOME`
2. `TOMCAT_HOME`
3. `ARCHAPPL_APPLIANCES`
4. `ARCHAPPL_MYIDENTITY`
5. `ARCHAPPL_SHORT_TERM_FOLDER` or equivalent
6. `ARCHAPPL_MEDIUM_TERM_FOLDER` or equivalent
7. `ARCHAPPL_LONG_TERM_FOLDER` or equivalent
8. `JAVA_OPTS` - This is the environment variable typically used by
   Tomcat to pass arguments to the VM. You can pass in appropriate
   arguments like so

   ```bash
   export JAVA_OPTS="-XX:+UseG1GC -Xmx4G -Xms4G -ea"
   ```

9. `LD_LIBRARY_PATH` - If you are using JCA, please make sure your
   LD_LIBRARY_PATH includes the paths to the JCA and EPICS base
   `.so`'s.

A sample startup script using these elements is available
[here](../samples/sampleStartup.sh). Please modify to suit your
installation.
