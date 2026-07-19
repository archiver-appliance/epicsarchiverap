# Contributing to the EPICS Archiver Appliance

Thanks for your interest in contributing! This guide covers how to build,
run, test, and format the EPICS Archiver Appliance and its documentation.

## Prerequisites

- Java JDK 21 (or later) — [OpenJDK](https://openjdk.java.net/) or another supplier
- [Gradle](https://gradle.org/) — optional; the `./gradlew` wrapper downloads a matching Gradle for you. To use a system `gradle`, match the major version of the wrapper in [gradle-wrapper.properties](https://github.com/archiver-appliance/epicsarchiverap/blob/master/gradle/wrapper/gradle-wrapper.properties).
- [EPICS base](https://github.com/epics-base/epics-base) — for the EPICS integration tests
Other useful tools:
- [Docker](https://www.docker.com/) and [Docker Compose](https://docs.docker.com/compose/)
- An IDE of your choice (e.g. IntelliJ, Eclipse, VS Code)

See the [system requirements](https://epicsarchiver.readthedocs.io/en/latest/sysadmin/references/system-requirements.html)
page for more detail on what is needed to build and test.

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

## Running the appliance locally

To run the application just as if it were in an integration test — for example
to manually try out a new development — use:

```bash
gradle testRun
```

This launches an embedded two-appliance cluster (no separate Tomcat install
needed) and stays in the console; press Ctrl-C to shut it down. Then open the
management apps at [appliance0](http://localhost:17665/mgmt) and
[appliance1](http://localhost:17666/mgmt) in a recent version of
Firefox/Google Chrome.

Alternatively, you can run the application in docker with compose, in a
single-container setup:

```bash
docker compose -f docker/docker-compose.single.yml up
```

or a multi-container setup:

```bash
docker compose -f docker/docker-compose.yml up
```

## Running the tests

The tests are organised into tags: "slow", "integration", "localEpics",
"flaky", "unit". Gradle creates temporary directories for all the unit tests;
run `gradle clean` first if you want to clear them.

### Unit tests

Unit tests are required for the build to complete:

```bash
gradle test        # fast unit tests (excludes the slow and flaky tags)
gradle unitTests   # all unit tests, including slow (flaky are run separately)
gradle flakyTests  # the flaky-tagged tests, which can fail on timing/machine specifics
```

To run a single test, use the `--tests` argument (this also works with the
other test tasks below):

```bash
gradle test --tests "org.epics.archiverappliance.TestName"
```

### Integration tests

Integration tests build and explode the WARs and run them against the embedded
Tomcat, so they only need the build itself — no Tomcat install. Tests that
additionally require a local [EPICS](https://epics-controls.org/) installation
are run with:

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

The embedded tomcats are torn down in-process when the task finishes; if you
cancel a run with Ctrl-C they stop with it.

To bring the application up interactively rather than as a test, see
[Running the appliance locally](#running-the-appliance-locally) above.

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
A Python virtual environment is managed automatically by Gradle.

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
