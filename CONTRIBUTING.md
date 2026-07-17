# Contributing to the EPICS Archiver Appliance

Thanks for your interest in contributing! This guide covers how to build,
run, test, and format the EPICS Archiver Appliance and its documentation.

## Prerequisites

- Java JDK 21 (or later) — [OpenJDK](https://openjdk.java.net/) or another supplier
- [Tomcat 11](https://tomcat.apache.org/download-11.cgi) — for the integration tests; the appliance supports up to Tomcat 11
- [Gradle](https://gradle.org/) — optional; the `./gradlew` wrapper downloads a matching Gradle for you. To use a system `gradle`, match the major version of the wrapper in [gradle-wrapper.properties](https://github.com/archiver-appliance/epicsarchiverap/blob/master/gradle/wrapper/gradle-wrapper.properties).
- [EPICS base](https://github.com/epics-base/epics-base) — for the EPICS integration tests

Other useful tools:

- [Docker](https://www.docker.com/) and [Docker Compose](https://docs.docker.com/compose/)
- An IDE of your choice (e.g. IntelliJ, Eclipse, VS Code)

See the [system requirements](https://epicsarchiver.readthedocs.io/en/latest/sysadmin/references/system-requirements.html)
page for more detail on what is needed to build and test.

An installation of Tomcat is required to build successfully; it is located
using the environment variable `TOMCAT_HOME`:

```bash
[ epicsarchiverap ]$ echo $TOMCAT_HOME
/opt/local/tomcat/latest
```

By default, Tomcat sets up an HTTP listener on port 8080. You can change this
in the Tomcat `server.xml` to avoid collision with other folks running Tomcat.
For example, here it is changed to 17665:

```xml
<Connector port="17665" protocol="HTTP/1.1"
				connectionTimeout="20000"
				redirectPort="8443" />
```

To run the unit tests, make a copy of your Tomcat configuration (preferably
pristine) into a new folder called `conf_original`. The unit tests that use
Tomcat copy the `conf_original` folder to generate new configurations for each
test. Gradle will do this step for you if you forget.

```bash
cd ${TOMCAT_HOME}
cp -R conf conf_original
```

## Building

We use [Gradle](http://gradle.org/) for building. The default target builds
the install package and the various wars and places them into the
`build/distributions` folder:

```bash
gradle
```

You don't need to install Gradle — use the wrapper `./gradlew` instead. The
build can then be found in `build/distributions`, or the war files in
`build/libs`. To build a site-specific customised version, set the environment
variable `ARCHAPPL_SITEID` to a folder name in `src/sitespecific` (there is an
example custom build in `src/sitespecific/slacdev`).

## Running Tomcat

Start Tomcat using the `catalina.sh run` or `catalina.sh start` commands, found
in the Tomcat `bin` folder. `catalina.sh run` leaves Tomcat running in the
console so that you can Ctrl-C to terminate; `catalina.sh start` runs it in the
background and you stop it with `catalina.sh stop`.

To bring up the management app, open
`http://<YourMachineHere>:17665/mgmt/ui/index.html` in a recent version of
Firefox/Google Chrome.

## Running the tests

The tests are organised into tags: "slow", "integration", "localEpics",
"flaky", "unit". Gradle creates temporary directories for all the unit tests;
run `gradle clean` first if you want to clear them.

### Unit tests

Unit tests are required for the build to complete:

```bash
gradle test        # all unit tests except slow tests
gradle unitTests   # all unit tests, including flaky and slow
```

To run a single test, use the `--tests` argument (this also works with the
other test tasks below):

```bash
gradle test --tests "org.epics.archiverappliance.TestName"
```

### Integration tests

Integration tests require an installation of Tomcat (up to version 11) with
`TOMCAT_HOME` set. Tests that require a local
[EPICS](https://epics-controls.org/) installation are run with:

```bash
gradle epicsTests
```

You can instead use a docker image containing EPICS:

```bash
docker compose -f docker/docker-compose.epicsTests.yml run epicsarchiver-test
```

The other integration tests produce a lot of data on disk, so it is advised
not to run them all at once. To run a single integration test:

```bash
gradle integrationTests --tests "org.epics.archiverappliance.retrieval.DataRetrievalServletTest"
```

If you cancel an integration test early, or it gets stuck, kill any running
tomcats with:

```bash
gradle shutdownAllTomcats
```

### Test run

To run the application just as if it were in an integration test — for example
to manually test a new development — use:

```bash
gradle testRun
```

Then access the running [appliance0](http://localhost:17665/mgmt) and
[appliance1](http://localhost:17666/mgmt). To shut down, interrupt the command
(Ctrl-C) and run `gradle shutdownAllTomcats`. Note this shuts down all tomcats
running, not just those created by `gradle testRun`.

## Formatting with Spotless

The gradle build script `build.gradle.kts` includes the
[Spotless Plugin](https://github.com/diffplug/spotless), which tracks the
formatting of the code (Java, and web code — HTML, CSS, JavaScript). New code
is checked against `origin/master` (in CI this may differ from your local
origin remote). To format new code:

```bash
gradle spotlessApply
```

Or to check the formatting is correct:

```bash
gradle spotlessCheck
```

Make sure your local `origin/master` is up to date with the
[home repository](https://github.com/archiver-appliance/epicsarchiverap) master
branch to pass the CI checks.

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
[ReadTheDocs](https://epicsarchiver.readthedocs.io/) and rebuilt automatically
on each push to the main branch. See the
[docs README](https://github.com/archiver-appliance/epicsarchiverap/blob/master/docs/README.md)
for more details.
