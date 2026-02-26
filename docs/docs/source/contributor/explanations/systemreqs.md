## System requirements

These are the prerequisites for the EPICS archiver appliance.

- A recent version of Linux, definitely 64 bit Linux for production
  systems. If using RedHat, we should aim for RedHat 6.1.
- JDK 1.16+ - definitely the 64 bit version for production systems. We
  need the JDK, **not** the JRE.
- A recent version of Tomcat 11.x; preferably `apache-tomcat-11.0.12` or
  later.
- The management UI works best with a recent version of Firefox or
  Chrome.
- By default, the EPICS archiver appliance uses a bundled versions of
  the Java CA and PVA libraries from [EPICS
  base](https://github.com/epics-base/epicsCoreJava).

Optionally, we\'d need

- A recent version of MySQL `mysql-5.1` or later if persisting
  configuration to a database. We hope to add Postgres support soon.

In terms of hardware, for production systems, we\'d need a reasonably
powerful server box with lots of memory for each appliance. For example,
we use 24 core machines with 128GB of memory and 15K SAS drives for
medium term storage.
