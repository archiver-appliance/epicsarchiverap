# Contributing to the EPICS Archiver Appliance

Thanks for your interest in contributing! This guide covers how to build,
run, test, and format the EPICS Archiver Appliance and its documentation.

## Prerequisites

Please see the [system requirements](https://epicsarchiver.readthedocs.io/en/latest/sysadmin/references/system-requirements.html)
page for prerequisites to build and test the EPICS Archiver Appliance. An
installation of Tomcat is required to build successfully; this is
located using the environment variable `TOMCAT_HOME`. Use something like

```bash
[ epicsarchiverap ]$ echo $TOMCAT_HOME
/opt/local/tomcat/latest
```

By default, Tomcat sets up a HTTP listener on port 8080. You can change
this in the Tomcat `server.xml` to avoid collision with other folks
running Tomcat. For example, here I have changed this to 17665.

```xml
<Connector port="17665" protocol="HTTP/1.1"
				connectionTimeout="20000"
				redirectPort="8443" />
```

To run the unit tests, please make a copy of your Tomcat configuration
(preferably pristine) into a new folder called `conf_original`. The unit
tests that use Tomcat copy the `conf_original` folder to generate new
configurations for each test.

```bash
cd ${TOMCAT_HOME}
cp -R conf conf_original
```

Gradle will do this step for you if you forget.

## Building

The EPICS archiver appliance is shared on
[GitHub](https://github.com/archiver-appliance/epicsarchiverap) using Git as
the source control repository. We use [Gradle](http://gradle.org/) for
building. The default target builds the install package and the various
wars and places them into the `build/distributions` folder.

```bash
$ ls build/distributions
archappl_v1.1.0-31-ge02e1f1.dirty.tar.gz
```

The Gradle build script will build into the default build directory
`build`. You don't need to install Gradle, instead you can use the
wrapper as `./gradlew`, or install it and run from the `epicsarchiverap`
folder:

```bash
$ gradle
BUILD SUCCESSFUL in 16s
12 actionable tasks: 10 executed, 2 up-to-date
```

The build can then be found in `epicsarchiverap/build/distributions` or
the war files in `epicsarchiverap/build/libs`.

## Running Tomcat

Start Tomcat using the `catalina.sh run` or the `catalina.sh start`
commands. The `catalina.sh` startup script is found in the Tomcat bin
folder. `catalina.sh run` starts Tomcat and leaves it running in the
console so that you can Ctrl-C to terminate. `catalina.sh start` starts
Tomcat in the background and you will need to run `catalina.sh stop` to
stop the process.

To bring up the management app, bring up
`http://<YourMachineHere>:17665/mgmt/ui/index.html` in a recent
version of Firefox/Google Chrome.

## Running the unit tests

Gradle creates temporary directories for all the unit tests. If you wish
to clean them first you can use `gradle clean`. You then have the
following options:

```bash
gradle test # Runs all unit tests except slow tests
gradle unitTests # Runs all unit tests
gradle epicsTests # Runs all integration tests that require only an epics installation
gradle integrationTests # Runs all tests that require a tomcat installation and optionally an epics installation
gradle flakyTests # Runs all tests that can fail due to system resources
gradle allTests # Runs all tests (not recommended)
```

Or run individual tests with:

```bash
gradle test -tests PolicyExecutionTest
gradle integrationTests --tests PvaGetArchivedPVsTest --info
```

If you cancel an integrationTest early, or it gets stuck for some reason
it's possible to kill any tomcats running with

```bash
gradle shutdownAllTomcats
```

If you wish to run the current development version locally for testing,
it's possible to use:

```bash
gradle testRun
```

## Formatting with Spotless

The gradle build script `build.gradle.kts` includes the [Spotless Plugin](https://github.com/diffplug/spotless)
which tracks the formatting of the code. To run the formatter run:

```bash
gradle spotlessApply
```

The build script checks that the changes in the current git branch are
up-to-date with the `origin/master` branch. So make sure your local
`origin/master` is up-to-date with the [home repository](https://github.com/archiver-appliance/epicsarchiverap)
master branch to pass the CI checks.

## Building the Documentation

The documentation is written in Markdown (MyST) and built with Sphinx.
A Python virtual environment is managed automatically by Gradle — no manual
Python setup required.

| Gradle task | Description |
|-------------|-------------|
| `./gradlew sphinx` | Build HTML docs into `docs/build/` |
| `./gradlew liveviewdocs` | Live-reload server at <http://127.0.0.1:8000> for development |
| `./gradlew javadoc sphinx` | Build including the Java API reference |

The generated docs are embedded in `mgmt.war` at `ui/help/` for local
application use. The published version is hosted at
[ReadTheDocs](https://epicsarchiver.readthedocs.io/) and rebuilt
automatically on each push to the main branch.
