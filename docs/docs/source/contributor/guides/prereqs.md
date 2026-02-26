## Prerequisites

Please see the [details](./details#system-requirements) page for
prerequisites to build and test the EPICS Archiver Appliance. An
installation of Tomcat is required to build successfully; this is
located using the environment variable `TOMCAT_HOME`. Use something like

```bash
[ epicsarchiverap ]$ echo $TOMCAT_HOME
/opt/local/tomcat/latest
[ epicsarchiverap ]$ ls -l $TOMCAT_HOME/
drwxr-x---   3 mshankar cd  4096 Oct 29 18:25 bin
-rw-r-----   1 mshankar cd 19182 May  3  2019 BUILDING.txt
drwx------   3 mshankar cd   254 Jul 29 14:41 conf
drwx------   2 mshankar cd   238 May 22 15:43 conf_from_install
drwxr-xr-x+  2 mshankar cd   238 May 22 15:44 conf_original
-rw-r-----   1 mshankar cd  5407 May  3  2019 CONTRIBUTING.md
drwxr-x---   2 mshankar cd  4096 Sep 17 18:13 lib
-rw-r-----   1 mshankar cd 57092 May  3  2019 LICENSE
drwxr-x---   2 mshankar cd   193 Nov 11 16:58 logs
-rw-r-----   1 mshankar cd  2333 May  3  2019 NOTICE
-rw-r-----   1 mshankar cd  3255 May  3  2019 README.md
-rw-r-----   1 mshankar cd  6852 May  3  2019 RELEASE-NOTES
-rw-r-----   1 mshankar cd 16262 May  3  2019 RUNNING.txt
drwxr-x---   2 mshankar cd    30 Sep 17 18:19 temp
drwxr-x---  11 mshankar cd   205 Nov 11 16:58 webapps
drwxr-x---   3 mshankar cd    22 May 22 15:55 work
[ epicsarchiverap ]$
```

By default, Tomcat sets up a HTTP listener on port 8080. You can change
this in the Tomcat server.xml to avoid collision with other folks
running Tomcat. For example, here I have changed this to 17665.

```xml
<Connector port="17665" protocol="HTTP/1.1"
                connectionTimeout="20000"
                redirectPort="8443" />
```

To run the unit tests, please make a copy of your Tomcat configuration
(preferably pristine) into a new folder called `conf_original.` The unit
tests that use Tomcat copy the conf_original folder to generate new
configurations for each test.

```bash
    cd ${TOMCAT_HOME}
    cp -R conf conf_original
```

Gradle will do this step for you if you forget.
