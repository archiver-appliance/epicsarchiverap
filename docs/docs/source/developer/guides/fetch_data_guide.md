# Retrieving data using the web client

The EPICS Archiver Appliance supports data retrieval in multiple
formats/MIME types. These are some of the few formats supported today;
more can easily be added as needed.

1. [JSON](http://www.json.org/) - A generic JSON format that can be
   easily loaded into most browsers using Javascript.
2. CSV - Can be used for importing into Excel and other spreadsheets.
3. MAT - This is the file format used for interoperating with Matlab.
4. RAW - This is a binary format used by the Archive Viewer and is
   based on the [PB/HTTP](pb_pbraw.html) protocol.
5. TXT - A simple text format that is often helpful for debugging.
6. [SVG](http://www.w3.org/Graphics/SVG/) - A XML format that can also
   be used as a SVG element in tools that support this format.

For GUI clients such as CS-Studio, Archive Viewer, and Matlab, see their
respective how-to guides: [CS-Studio](../../operator/guides/cstudio),
[Archive Viewer](../../operator/guides/archiveviewer), [Matlab](../../operator/guides/matlab).

## Constructing a data retrieval URL

Getting data into a tool necessitates construction of a data retrieval
URL as the first step. A data retrieval URL looks something like so

<http://archiver.slac.stanford.edu/retrieval/data/getData.json?pv=VPIO%3AIN20%3A111%3AVRAW&from=2012-09-27T08%3A00%3A00.000Z&to=2012-09-28T08%3A00%3A00.000Z>

where

1. `http://archiver.slac.stanford.edu/retrieval` is the
   `data_retrieval_url` element from your `appliances.xml`
2. `/data/getData` is the path into the data retrieval servlet and is
   fixed.
3. `.json` identifies the MIME-type of the returned data.
4. The remainder consists of parameters (all of which need to be [URL
   encoded](http://en.wikipedia.org/wiki/Percent-encoding) for
   correctness)
   1. `pv` - This identifies the name of the PV for which data is
      requested.
   2. `from` - This is the start time of the data in [ISO
      8601](http://en.wikipedia.org/wiki/ISO_8601) format;
      specifically
      [this](<http://joda-time.sourceforge.net/apidocs/org/joda/time/format/ISODateTimeFormat.html#dateTime()>)
      format.
   3. `to` - This is the end time of the data in the same format.

For optional request parameters and the full response field reference,
see [Retrieval API](../references/retrieval-api.md).

## Example: fetching data in Python

Here's an example of loading data into Python in a Jupyter notebook and using Bokeh to display the data

```python
from datetime import datetime, timedelta
import pytz
import json

import requests
from bokeh.plotting import figure, show
from bokeh.io import output_notebook

utc = pytz.utc
tz = pytz.timezone('America/Los_Angeles')
end = datetime.now().astimezone(utc)
start = end - timedelta(days=1)
pvName = "ROOM:BSY0:1:OUTSIDETEMP"

resp = requests.get("http://archappl.epics-controls.org/retrieval/data/getData.json",
        params={
            "pv": pvName,
            "from": start.strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
            "to": end.strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
        })
resp.raise_for_status()
samples = resp.json()[0]["data"]
egu = resp.json()[0].get("meta", {}).get("EGU", "")
xpoints = [datetime.fromtimestamp(x["secs"]).astimezone(tz) for x in samples]
ypoints = [x["val"] for x in samples]

output_notebook()

fig = figure(title=pvName, x_axis_label='Time', x_axis_type='datetime', y_axis_label=egu)
fig.line(xpoints, ypoints)
show(fig)
```

![image](../../images/bokeh_in_jupyter.png)
