## Scripting

The archiver appliance comes with a web UI that has support for various
business processes like adding PV\'s to the archivers etc. The web UI
communicates with the server principally using JSON/HTTP web service
calls. The same web service calls are also available for use from
external scripting tools like Python.

```bash

#!/usr/bin/env python

import requests

resp = requests.get("http://archappl.slac.stanford.edu/mgmt/bpl/getAllPVs?pv=VPIO:IN20:111:VRA*")
print("\n".join(resp.json()))
```

Click [here](../references/mgmt_scriptables.md) for a list of
business logic accessible thru scripting.
