# Quick Start Guide

The steps outlined here should get you started quickly with evaluating
and testing a new archiver appliance. Note, **these steps are not meant
for production deployments**, but are meant for evaluating and getting
to know the system. For more details on how to deploy in a cluster or in
a production environment, please see the
[Installation](installguide) guide.

For folks familiar with deployment of WAR files, the archiver appliance
consists of 4 WAR files. These steps deploy these WAR files on a single
Tomcat JVM, set the log levels to ERROR and start the VM. To simplify
matters, we are not persisting configuration and therefore there are no
steps to define a connection pool for the configuration database.

Here are the steps to get started quickly.

1. Make sure you have a recent version of JDK by running
   `java -version`. You should see something like so

   ```bash
   $ java -version
   openjdk version "16" 2021-03-16
   OpenJDK Runtime Environment (build 16+36-2231)
   OpenJDK 64-Bit Server VM (build 16+36-2231, mixed mode, sharing)$
   ```

2. Download the installation package to a Linux machine into a brand
   new folder. This should give you a tar.gz file like
   `archappl_vx.x.x.tar.gz`.

3. Download a recent version of Tomcat 9.x. into the same folder. You
   should now have two files in this folder like so

   ```bash
   $ ls -l
   total 160996
   -rw-r--r-- 1 mshankar cd  10851264 Nov 13 08:08 apache-tomcat-9.0.20.tar.gz
   -rw-r--r-- 1 mshankar cd 154003400 Nov 13 08:08 archappl_v0.0.1_SNAPSHOT_12-November-2019T21-10-12.tar.gz
   ```

4. Untar the `archappl_vx.x.x.tar.gz`. This should untar into 4 WAR
   files and a bash script like so

   ```bash
   $ tar zxf archappl_v0.0.1_SNAPSHOT_12-November-2019T21-10-12.tar.gz
   $ ls -l
   total 312440
   -rw-r--r-- 1 mshankar cd     11358 Sep  9 16:03 Apache_2.0_License.txt
   -rw-r--r-- 1 mshankar cd  10851264 Nov 13 08:08 apache-tomcat-9.0.20.tar.gz
   -rw-r--r-- 1 mshankar cd 154003400 Nov 13 08:08 archappl_v0.0.1_SNAPSHOT_12-November-2019T21-10-12.tar.gz
   -rw-r--r-- 1 mshankar cd  37665101 Nov 12 21:10 engine.war
   -rw-r--r-- 1 mshankar cd  36181760 Nov 12 21:10 etl.war
   drwxr-xr-x 2 mshankar cd       148 Nov 13 08:10 install_scripts
   -rw-r--r-- 1 mshankar cd      3520 Sep  9 16:03 LICENSE
   -rw-r--r-- 1 mshankar cd  43139397 Nov 12 21:10 mgmt.war
   -rw-r--r-- 1 mshankar cd      2009 Sep  9 16:03 NOTICE
   -rwxr-xr-x 1 mshankar cd      8862 Sep  9 16:03 quickstart.sh
   -rw-r--r-- 1 mshankar cd       141 Nov 12 21:10 RELEASE_NOTES
   -rw-r--r-- 1 mshankar cd  38045560 Nov 12 21:10 retrieval.war
   drwxr-xr-x 3 mshankar cd        46 Nov 13 08:10 sample_site_specific_content
   $
   ```

5. Run the script like so

   ```bash
   $ ./quickstart.sh apache-tomcat-11.0.12.tar.gz
   ```

6. This should start the Tomcat process in the foreground. Once all the
   webapps have been initialized (it takes about 2-5 minutes), you
   should see a log message in the console
   `All components in this appliance have started up. We should be ready to start accepting UI requests`{.sample}
   like so

   ```bash
   ... INFO  config.org.epics.archiverappliance.config.DefaultConfigService  - Start complete for webapp ENGINE
   ... INFO  config.org.epics.archiverappliance.config.DefaultConfigService  - Start complete for webapp ETL
   ... INFO  config.org.epics.archiverappliance.config.DefaultConfigService  - Start complete for webapp RETRIEVAL
   ... INFO  config.org.epics.archiverappliance.mgmt.MgmtRuntimeState  - All components in this appliance have started up. We should be ready to start accepting UI requests
   ```

7. Open a browser to
   `http://<<`_`machinename`_`>>:17665/mgmt/ui/index.html` and you
   should see the home screen for your archiver appliance.

8. If your EPICS environment variables are set up correctly, you should
   be able to start archiving PV\'s right away. Note it takes about 5
   minutes for the archiver appliance to measure the event rate,
   storage rate etc and to transition PVs from the `Initial sampling`
   state to the `Being archived` state.

9. To stop the appliance, use a `CTRL-C` in the console.

Notes

- We set the Log4j root logging level to
  [ERROR](https://logging.apache.org/log4j/2.x/javadoc/log4j-api/org/apache/logging/log4j/Level.html#ERROR)
  by default. You should not see any exceptions or ERROR messages in
  the console on startup.
- We do not persist configuration in this setup. That is, if you kill
  the Tomcat process, you\'ll need to resubmit your PV\'s to the
  archiver on restart.
- You can increase the verbosity of console messages by passing in a
  `-v` _(for verbose)_ argument to startup script. The sets the Log4j
  root logging level to
  [DEBUG](https://logging.apache.org/log4j/2.x/javadoc/log4j-api/org/apache/logging/log4j/Level.html#DEBUG).

## Quickstart using predefined VMs

An alternate way to quickly get started and test the system is to use
the predefined VM\'s provided by Martin. These consists of three repos
needed to set up an EPICS Archiver Appliance test environment:

1. [vagrant_archiver_appliance](https://stash.nscl.msu.edu/projects/DEPLOY/repos/vagrant_archiver_appliance)
2. [puppet_module_archiver_appliance](https://stash.nscl.msu.edu/projects/DEPLOY/repos/puppet_module_archiver_appliance)
3. [puppet_module_epics_softioc](https://stash.nscl.msu.edu/projects/DEPLOY/repos/puppet_module_epics_softioc)

Simply follow the rules in the README of the first repo and the other
two repos will be pulled in automatically.
