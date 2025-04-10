# Epics Archiver Appliance

This is an implementation of an archiver for EPICS control systems that aims to archive millions of PVs.
At a high level, some features are
- Ability to cluster appliances and to scale by adding appliances to the cluster.
- Multiple stages and an inbuilt process to move data between the stages.
- Focus on data retrieval performance.
- Focus on zero oversight.

For more details, please see the [documentation](http://epicsarchiver.readthedocs.io/), or the [current branch documentation](docs).

## Development

The archiver appliance uses the [gradle build tool](https://gradle.org/).
You can use the gradle wrapper command `./gradlew`, which downloads gradle for you. Or you can install
a gradle version that has a version > the version of the wrapper in
[gradle wrapper properties](gradle-wrapper.properties) and use the `gradle` command.

### Build

To build simply run:

```bash
gradle
```

The resulting distribution can then be found in the `build/distributions` folder. To build for a sitespecific customised
version, set the environment variable ARCHAPPL_SITEID to a folder name in `src/sitespecific`. There is an example
custom build in `src/sitespecific/slacdev`.

### Formatting

This project uses [spotless](https://github.com/diffplug/spotless) to aim towards a standardised formatting.
New code is checked against "origin/master" (when run in CI this may be different to your local origin remote).
Currently, this includes Java code, and web code (HTML, CSS, javascript).

To format new code run:

```bash
gradle spotlessApply
```

Or to check the formatting is correct run:

```bash
gradle spotlessCheck
```

### Test

The tests are organised into different tags: "slow", "integration", "localEpics", "flaky", "singleFork", "unit".

#### Unit Tests
To run unit tests (these are required for the build process to complete):

```bash
gradle test
```

To run a single test, for example "TestName":

```bash
gradle test --tests "org.epics.archiverappliance.TestName"
```

The rest of the commands below can also be used with the `--tests` arguement to run a single test.

To run including "flaky" and "slow" tests:

```bash
gradle unitTests
```

#### Integration Tests

Integration tests require an installation of Tomcat (the Archiver Appliance only support up to version 9).
The environment variable `TOMCAT_HOME` where a "conf" folder exists must then be set.

To run tests requiring a local installation of [EPICS](https://epics-controls.org/).

```bash
gradle epicsTests
```

It's possible to use a docker image containing EPICS to run the epics tests:

```bash
docker compose -f docker/docker-compose.epicsTests.yml run epicsarchiver-test
```

The other integration tests produce a lot of data on disk, and it's advised to not run them all at once.
To run a single integration test:

```bash
gradle integrationTests --tests "org.epics.archiverappliance.retrieval.DataRetrievalServletTest"
```

#### Test Run

To run the application just as if it were in an integration test, for example to manually test a new development.
You can use the command:

```bash
gradle testRun
```

And then access the running [appliance0](http://localhost:17665/mgmt) and [appliance1](http://localhost:17666/mgmt).
To shut down the application, interrupt the command (using Ctrl-c for example) then you can run:

```bash
gradle shutdownAllTomcats
```

Note this will shut down all tomcats running, not just any created with `gradle testRun`. It's useful to run this
command if you interrupt an integration test as well.

## Deploy

For a quick deploy of a single appliance:

1. Download the latest [release](https://github.com/archiver-appliance/epicsarchiverap/releases)
	and unpack the war files to a folder "archiver".
2. In the same folder, download a release of [tomcat 9](https://tomcat.apache.org/download-90.cgi) without unpacking.
3. Copy the quickstart.sh file from the extracted release to the folder.
4. Run the quickstart script:

```bash
./quickstart.sh  apache-tomcat-9.*.tar.gz
```

For more information see the [quickstart documentation](https://epicsarchiver.readthedocs.io/en/latest/sysadmin/quickstart.html). For more complicated
deployments see the [samples folder](docs/samples).

## Build Documentation

Documentation for the website is built using [Read the Docs](http://readthedocs.org).
To build it and run it locally:

```bash
cd docs
python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade --no-cache-dir pip setuptools
python -m pip install --upgrade --no-cache-dir sphinx readthedocs-sphinx-ext
python -m pip install --exists-action=w --no-cache-dir -r docs/requirements.txt
cd docs
sphinx-autobuild source build
```
