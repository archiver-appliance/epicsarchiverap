# Setting up Tomcat

Installing Tomcat consists of

1. Untar'ing the Tomcat distribution. It is best to set the
   environment variable `TOMCAT_HOME` to the location where the Tomcat
   distribution is expanded. Many of the following steps require a
   `TOMCAT_HOME` to be set.

2. Editing the `conf/server.xml` file to change the ports to better
   suit your installation.

   1. By default, the connector port for the HTTP connector is set
      to 8080. Change this to the port used by the `mgmt` webapp for
      this appliance, in this example, 17665.

      ```xml
      <Connector connectionTimeout="20000" port="17665" protocol="HTTP/1.1" redirectPort="8443"/>
      ```

   2. Remove/comment out the sections for the AJP connector.

   3. At the end, there should be two ports active in the
      `conf/server.xml` file, one for the HTTP connector and the other
      for the `SHUTDOWN` command.

3. Setting the appropriate log4j configuration level by
   creating/editing the `lib/log4j2.xml`. Here's a sample that logs
   exceptions and errors with one exception - log messages logged to
   the `config` namespace are logged at INFO level.

   ```xml
   <Configuration>
      <Appenders>
           <Console name="STDOUT" target="SYSTEM_OUT">
               <PatternLayout pattern="%d %-5p [%t] %C{2} (%F:%L) - %m%n"/>
           </Console>
       </Appenders>
       <Loggers>
           <Logger name="org.apache.log4j.xml" level="info"/>
           <Root level="info">
               <AppenderRef ref="STDOUT"/>
           </Root>
       </Loggers>
   </Configuration>
   ```

4. To use [Apache Commons Daemon](http://commons.apache.org/daemon/),
   unzip the `${TOMCAT_HOME}/bin/commons-daemon-native.tar.gz` and
   follow the instructions. Once you have built this, copy the `jsvc`
   binary to the Tomcat `bin` folder for convenience. Note, it's not
   required that you use `Apache Commons Daemon` especially, if you are
   already using system monitoring and management tools like
   [Nagios](http://www.nagios.org/) or
   [Hyperic](http://www.hyperic.com/).

   ```bash
   $ tar zxf commons-daemon-native.tar.gz
   $ cd commons-daemon-1.1.0-native-src
   $ cd unix/
   $ ./configure
   ...
   $ make
   ...
   $ cp jsvc ../../../bin/
   ```
