# Documentation for the EPICS Archiver Appliance

Published at <https://epicsarchiver.readthedocs.io/> via ReadTheDocs.

## Building locally

Documentation is built via Gradle, which manages a Python virtual environment
automatically — no manual Python setup required.

| Command | Description |
|---------|-------------|
| `./gradlew sphinx` | Build HTML documentation into `docs/build/` |
| `./gradlew liveviewdocs` | Start live-reload server at <http://127.0.0.1:8000> (auto-rebuilds on file changes) |
| `./gradlew javadoc` | Generate Java API docs (must run before `sphinx` to include API reference) |
| `./gradlew mgmtWar` | Build the mgmt WAR with docs embedded at `ui/help/` |

The first run of `sphinx` or `liveviewdocs` will create `docs/.venv/` and install
all dependencies from `docs/pyproject.toml` automatically.
