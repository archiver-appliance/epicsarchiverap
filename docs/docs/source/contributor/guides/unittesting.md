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
it\'s possible to kill any tomcats running with

```bash
gradle shutdownAllTomcats
```

If you wish to run the current development version locally for testing,
it\'s possible to use:

```bash
gradle testRun
```
