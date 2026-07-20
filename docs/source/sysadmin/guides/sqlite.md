# Using SQLite as a configuration database

For smaller installations with lighter concurrent access, one can
consider using SQLite as a configuration database.

1. Copy the SQLite JDBC from <https://github.com/xerial/sqlite-jdbc> to
   the `${TOMCAT_HOME}/lib` folder.

   ```bash
   $ cd $TOMCAT_HOME/lib
   $ ls sqlite-jdbc-3.39.3.0.jar
   sqlite-jdbc-3.39.3.0.jar
   ```

2. Configure a connection pool using SQLite in the Tomcat context.xml

   ```bash
   $ cd $TOMCAT_HOME/conf
   $ cat context.xml
   <?xml version="1.0" encoding="UTF-8"?>
   ...
       <Resource   name="jdbc/archappl"
                   auth="Container"
                   type="javax.sql.DataSource"
                   username="xxx"
                   password="xxx"
                   maxTotal="1"
                   maxIdle="1"
                   maxActive="1"
                   driverClassName="org.sqlite.JDBC"
                   url="jdbc:sqlite:/archappl/config/archappl.sqlite?journal_mode=WAL"
                   />
   ...

   ```

   1. Note that sqlite requires both the DB file and the containing
      directory to be writable.

   2. SQLite does file locking during updates; so we cannot really use
      multiple connections in the connection pool. This requires us
      setting the `maxActive`, `maxIdle` and `maxTotal` to 1.

   3. If more than one connection tries to write to the DB at the same
      time, we\'d see exceptions of the following nature

      ```bash
      ERROR org.epics.archiverappliance.config.DefaultConfigService  - Exception persisting pvTypeInfo for pv ...
      java.io.IOException: org.sqlite.SQLiteException: [SQLITE_BUSY] The database file is locked (database is locked)
      ```

3. Initialize the SQLite database using the
   `install/archappl_sqlite.sql` SQL script shipped as part of the
   `mgmt.war`

   ```bash
   $ cd /archappl/config/
   $ sqlite3 --init ~/unzipped_mgmt_war/install/archappl_sqlite.sql archappl.sqlite
   -- Loading resources from install/archappl_sqlite.sql

   SQLite version 3.7.17 2013-05-20 00:56:22
   Enter ".help" for instructions
   Enter SQL statements terminated with a ";"
   sqlite> .quit
   $
   ```

4. It is recommended to use the SQLite
   [WAL](https://www.sqlite.org/wal.html) journal mode to improve write
   performance. Using WAL usually implies the presence of a .shm and
   .wal file in addition to the .sqlite file.
