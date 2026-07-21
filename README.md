# Epics Archiver Appliance

This is an implementation of an archiver for EPICS control systems that aims to archive millions of PVs.
At a high level, some features are

- Ability to cluster appliances and to scale by adding appliances to the cluster.
- Multiple stages and an inbuilt process to move data between the stages.
- Focus on data retrieval performance.
- Focus on zero oversight.

For more details, please see the [documentation](http://epicsarchiver.readthedocs.io/), or the [current branch documentation](docs).

See [CONTRIBUTING.md](CONTRIBUTING.md) for how to build, run, test and format the
appliance, and [ARCHITECTURE.md](ARCHITECTURE.md) for an overview of how it is put
together.

## Deploy

For a quick deploy of a single appliance:

1. Download the latest [release](https://github.com/archiver-appliance/epicsarchiverap/releases)
and unpack the war files to a folder "archiver".
2. In the same folder, download a release of [tomcat 11](https://tomcat.apache.org/download-11.cgi) without unpacking.
3. Copy the quickstart.sh file from the extracted release to the folder.
4. Run the quickstart script:

```bash
./quickstart.sh  apache-tomcat-11.*.tar.gz
```

For more information see the [quickstart documentation](https://epicsarchiver.readthedocs.io/en/latest/sysadmin/tutorials/quickstart.html). For more complicated
deployments see the [samples folder](docs/source/samples).
