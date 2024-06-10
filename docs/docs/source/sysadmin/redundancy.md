# Redundancy in the EPICS Archiver Appliance

The EPICS Archiver Appliance has limited support for archiving the same
PV in multiple clusters and merging in the data from both appliances
during data retrieval. This feature allows for a some redundancy when
archiving a small set of critical PVs. At a high level

- Archive the same PV in two independent clusters.
  - The two clusters need not have the same policy. For example, you
        can designate one cluster the **primary** cluster and the other
        one the **secondary** cluster
  - The primary cluster can archive the PV using your usual policies
        while the secondary can store data for a much smaller timeframe.
- Configure one of the clusters ( for performance reasons, the smaller
    of the two, most likely the secondary ) to proxy the other one.
  - When creating the proxy, add a param `mergeDuringRetrieval`. For
        example, add a *External EPICS Archiver Appliance* proxy with a
        URL that looks like so
        `http://archapp.slac.stanford.edu/retrieval?mergeDuringRetrieval=true`
- Periodically, if needed, merge in the data manually from the
    secondary cluster to the primary using the `mergeInData` BPL.

## Case study

In an ideal world, to achive redundancy when archiving PV\'s, we\'d have
multiple large identical independent clusters archiving the same set of
PVs. For financial and other reasons, this may not be possible for all
installations. This case study outlines a setup for achieving redundancy
for a small subset of critical PVs.

Because we wish to accomplish redundancy for a small set of critical
PVs, we have two asymmetric independent clusters. The **primary**
cluster is your main archiver and will archive millions of PV\'s (
including the small subset of critical PVs ) and process almost all the
data retrieval requests. The **secondary** cluster is an independent
cluster and is the backup archiver and will archive only the small
subset of critical PVs. In this case, the data is stored only for 6
months and then is automatically deleted using a blackhole plugin.

When we add the `mergeDuringRetrieval` proxy to the **secondary**
cluster, all data retrieval requests to the **secondary** cluster will
automatically make a call to the **primary** cluster with the same
parameters and then merge in the data from both clusters. Note that this
does not alter the stored data in any way; the merge is only done for
data retrieval. Thus, data retrieval calls to the **secondary** cluster
will also include data from the **primary** cluster. Folks interested in
a complete data set can make data retrieval calls to the **secondary**
cluster; the data retrieval will be slightly slower because of the
merging operation. Folks making data retrieval calls to the **primary**
will only get data from the primary cluster; but because no merging is
performed, retrieval calls are much faster.

Periodically, one can manually merge in data from the **secondary**
cluster to the **primary** cluster using the `mergeInData` BPL. This
requires data the PV to be paused in the **primary** cluster while the
merge is happening. The `mergeInData` picks up all data from the
**secondary** cluster and merges it into the **primary** cluster.
