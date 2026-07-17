# Installing MySQL

The version of MySQL that is available from your distribution is
acceptable; though this is completely untuned. Please look at the more
than excellent chapters on MySQL optimization at the MySQL web site to
tune your MySQL instance. In addition to various parameters, even
something as simple as setting `innodb_flush_log_at_trx_commit=0`
(assuming you are ok with this) will go a long way in improving
performance (especially when importing channel archiver configuration
files etc). Each appliance has its own installation of MySQL. In each
appliance,

- Make sure MySQL is set to start on powerup (using `chkconfig`)

- Create a schema for the archiver appliance called `archappl` and
  grant a user (in this example, also called `archappl`) permissions
  for this schema.

  ```sql
  CREATE DATABASE archappl;
  GRANT ALL ON archappl.* TO 'archappl'@localhost IDENTIFIED BY '<password>';
  ```

- The archiver appliance ships with DDL, for MySQL, this is a file
  called `archappl_mysql.sql` that is included as part of the `mgmt`
  WAR file. Execute this script in you newly created schema. Confirm
  that the tables have been created using a `SHOW TABLES` command.
  There should be at least these tables

  1. `PVTypeInfo` - This table stores the archiving parameters for
     the PVs
  2. `PVAliases` - This table stores EPICS alias mappings
  3. `ExternalDataServers` - This table stores information about
     external data servers.
  4. `ArchivePVRequests` - This table stores archive requests that
     are still pending.

- Download and install the [MySQL
  Connector/J](http://dev.mysql.com/downloads/connector/j/) jar file
  into your Tomcat's `lib` folder. In addition to the log4j2.xml
  file, you should have a `mysql-connector-java-XXX.jar` as shown here.

  ```bash
  $ ls -ltra
  ...
  -rw-r--r-- 1 mshankar cd     505 Nov 13 10:29 log4j2.xml
  -rw-r--r-- 1 mshankar cd 1007505 Nov 13 10:29 mysql-connector-java-5.1.47-bin.jar
  ```

- Add a connection pool in Tomcat named `jdbc/archappl`. You can use
  the Tomcat management UI or directly add an entry in
  `conf/context.xml` like so

  ```xml
  <Resource   name="jdbc/archappl"
        auth="Container"
        type="javax.sql.DataSource"
        factory="org.apache.tomcat.jdbc.pool.DataSourceFactory"
        username="archappl"
        password="XXXXXXX"
        testWhileIdle="true"
        testOnBorrow="true"
        testOnReturn="false"
        validationQuery="SELECT 1"
        validationInterval="30000"
        timeBetweenEvictionRunsMillis="30000"
        maxActive="10"
        minIdle="2"
        maxWait="10000"
        initialSize="2"
        removeAbandonedTimeout="60"
        removeAbandoned="true"
        logAbandoned="true"
        minEvictableIdleTimeMillis="30000"
        jmxEnabled="true"
        driverClassName="com.mysql.jdbc.Driver"
        url="jdbc:mysql://localhost:3306/archappl"
   />
  ```

  Of course, please do make changes appropriate to your installation.
  The only parameter that is fixed is the name of the pool and this
  needs to be `jdbc/archappl`. All other parameters are left to your
  discretion.

  - Note for Debian/Ubuntu users: The Tomcat packages shipped with
    Debian/Ubuntu do not include the Tomcat JDBC Connection Pool.
    Download it from the web and drop the `tomcat-jdbc.jar` file
    into `/usr/share/tomcat7/lib`.
