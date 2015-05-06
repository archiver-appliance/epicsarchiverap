#!/usr/bin/env python
import numpy as np
from chaco.shell import *
import urllib2
import json

req = urllib2.urlopen("http://archiver.slac.stanford.edu:17665/retrieval/data/getData.json?pv=mshankar%3Aarch%3Asine&donotchunk")
data = json.load(req)
secs = [x['secs'] for x in data[0]['data']]
vals = [x['val'] for x in data[0]['data']]
plot(secs, vals, "r-")
xscale('time')
show()

