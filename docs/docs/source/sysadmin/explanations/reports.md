# Reports and metrics

## Reports

The EPICS Archiver Appliance supports a wide variety of reports based on
static and dynamic information. These reports can also be accessed from
within scripting languages like python and can be used to facilitate
automation and integration. Some reports that are currently available
are

- **PV's that may not exist** - This report lists all the
  PVs in the cluster that have never connected. PVs whose names have
  typos in them would be included in this report.
- **Currently disconnected PVs** - This report lists all
  the PVs in the cluster that have connected in the past but are
  currently disconnected; perhaps because their IOCs have been turned
  off.
- **Top PVs by event rate** - These reports contain the PVs
  sorted by their actual event rate (descending).
- **Top PVs by storage rate** - These reports contain the
  PVs sorted by their actual storage rate (descending).
- **Recently added PVs** - These reports contain the PVs
  sorted by the creation timestamp of their PVTypeInfos (descending).
  PVs that were recently added show up first.
- **Recently modified PVs** - These reports contain the PVs
  sorted by the modification timestamp of their PVTypeInfos
  (descending). PVs that were recently modified show up first.
- **PVs by lost/regained connections** - These reports
  contain the PVs sorted by the number of times we've
  lost/re-established the CA connection. This can be used to determine
  if an IOC is being overwhelmed (perhaps by all the archiving
  targeted at it).
- **PVs by last known timestamp** - These reports contain
  the PVs sorted by the timestamp of the last event received.
- **PVs by dropped events from incorrect timestamps** - The
  EPICS archiver appliance discards events that have invalid
  timestamps. These reports contain the PVs sorted by the number of
  events dropped because they had invalid timestamps. Events are
  dropped in these conditions.
  1. If the record processing timestamp from the IOC is before the
     PAST_CUTOFF_TIMESTAMP. PAST_CUTOFF_TIMESTAMP defaults to
     `1991-01-01T00:00:00.000Z`
  2. If the record processing timestamp from the IOC is after
     (Appliance Current Time + SERVER_IOC_DRIFT_SECONDS) in the future.
     SERVER_IOC_DRIFT_SECONDS defaults to 30\*60.
  3. If the record processing timestamp from the IOC is before the
     timestamp of the previous sample.
  4. If the record processing timestamp from the IOC is identical to
     the timestamp of the previous sample.
  5. If the record processing timestamp from the IOC of samples
     from the second sample onwards are before the
     (Appliance Current Time - SERVER_IOC_DRIFT_SECONDS) in the past.
- **PVs by dropped events from buffer overflows** - The
  EPICS archiver appliance discards events when the sampling buffers
  (as estimated from the sampling period) become full. These reports
  contain the PVs sorted by the number of events dropped because the
  sampling buffers were full.

## Metrics

The EPICS Archiver Appliance maintains a wide variety of metrics to
facilitate in capacity planning and load balancing. These metrics can be
viewed in the Metrics page; a small subset of these metrics can be
viewed across the cluster. To view more details about the metrics on a
particular appliance, click on that appliance in the list view.

## Storage

The EPICS Archiver Appliance supports multiple stages for storing data
and allows for configuration of data stores on a per PV basis. The
storage consumed across all PVs can be viewed in the Storage page.

## Appliances

The EPICS Archiver Appliance maintains a days worth of CPU/heap usage
statistics on a per appliance basis. These statistics can be viewed on
the Appliances page.

## Integrations

The EPICS Archiver Appliance supports limited integration with existing
Channel Archiver installations.

- To import ChannelArchiver `XML` configuration files, click on the
  `Choose Files` button, select any number of ChannelArchiver `XML`
  configuration files and press `Upload`.
  ![image](../images/importCAConfig.png)

  The `DTD` for the ChannelArchiver `XML` file can be found in the
  ChannelArchiver documentation or in the ChannelArchiver source
  distribution.

- To proxy data from existing ChannelArchiver `XML-RPC` data servers,
  add the URL to the `XML-RPC` data server using the `Add` button. The
  EPICS Archiver Appliance uses the `archiver.names` method to
  determine the PVs that are hosted by the ChannelArchiver `XML-RPC`
  data server and adds this server as a data source for these PVs.
