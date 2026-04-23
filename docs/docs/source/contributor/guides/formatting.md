## Formatting with Spotless

The gradle build script `build.gradle` includes the [Spotless Plugin](https://github.com/diffplug/spotless)
which tracks the
formatting of the code. To run the formatter run:

```bash
gradle spotlessApply
```

The build script checks that the changes in the current git branch are
up-to-date with `origin/master` branch. So make sure your local
`origin/master` is up-to-date with the [home repository](https://github.com/slacmshankar/epicsarchiverap) master
branch to pass the CI checks.
