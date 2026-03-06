## Building

The EPICS archiver appliance is shared on
[GitHub](https://github.com/slacmshankar/epicsarchiverap) using Git as
the source control repository. We use [Gradle](http://gradle.org/) for
building. The default target builds the install package and the various
wars and places them into the `build/distributions` folder.

```bash
$ ls build/distributions
archappl_v1.1.0-31-ge02e1f1.dirty.tar.gz
```

The Gradle build script will build into the default build directory
`build`. You don\'t need to install Gradle, instead you can use the
wrapper as `./gradlew`, or install it and run from the `epicsarchiverap`
folder:

```bash
$ gradle
BUILD SUCCESSFUL in 16s
12 actionable tasks: 10 executed, 2 up-to-date
```

The build can then be found in `epicsarchiverap/build/distributions` or
the war files in `epicsarchiverap/build/libs`.
