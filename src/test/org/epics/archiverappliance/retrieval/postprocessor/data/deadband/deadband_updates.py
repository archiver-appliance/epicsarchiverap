#!/usr/bin/env python

from cothread import Sleep
from cothread.catools import *

NAN=float('nan')
INFP=float('inf')
INFN=float('-inf')

alm='TST-CT{}Alrm-SP'
sig='TST-CT{}Sig:1-I'

# Reset
caput(alm, 0, wait=True)
caput(sig, 0, wait=True)
Sleep(1)

# Swing the value up 0->9
for N in range(10):
    caput(sig, N, wait=True)
Sleep(1)

# move around within deadband
caput(sig, 8, wait=True)
caput(sig, 9, wait=True)
caput(sig, 10, wait=True)
caput(sig, 8, wait=True)

caput(sig, 7, wait=True)
caput(sig, 6, wait=True)
Sleep(1)

# Move withing DB, but trigger alarm change
caput(sig, 5, wait=True)
caput(alm, 1, wait=True)
Sleep(1)

# changes to alarm severity
caput(alm, 0, wait=True)
caput(alm, 2, wait=True)
caput(alm, 3, wait=True)

caput(alm, 0, wait=True)
caput(sig, 6, wait=True)
Sleep(1)

# Test non-finite float values
caput(sig, NAN, wait=True)
caput(sig, NAN, wait=True)

caput(sig, 6, wait=True)
caput(sig, 9, wait=True)

caput(sig, INFP, wait=True)
caput(sig, INFP, wait=True)

caput(sig, 7, wait=True)

caput(sig, INFN, wait=True)
caput(sig, INFP, wait=True)
caput(sig, NAN, wait=True)

caput(sig, 8, wait=True)

# Return to 1, then disconnect
# reconnect will be 0
caput(sig, 1, wait=True)
Sleep(1)
