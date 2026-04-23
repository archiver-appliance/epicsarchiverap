# Retrieval API

Reference for the EPICS Archiver Appliance data retrieval HTTP API.
For a worked example of constructing URLs and fetching data, see
[Fetch data](../guides/fetch_data_guide.md).

## Optional request parameters

In addition to the required `pv`, `from`, and `to` parameters, the
following optional parameters are accepted by the `getData` endpoint.

`fetchLatestMetadata`
: If `true`, an extra call is made to the engine as part of the
retrieval to get the latest values of the various fields
_(DESC, HIHI etc)_.

`retiredPVTemplate`
: If specified, the archiving information (PVTypeInfo) for the PV
specified in this parameter is used as a template for PVs that do not
exist in the system. This is intended principally for use with legacy
PVs (PVs that no longer exist on the LAN and do not have a
PVTypeInfo).

1. For example, all the data for legacy PVs could be consolidated
   into yearly partitions and stored on tape.
2. When a user places a request for this data, it could be restored
   to some standard folder, for example _/arch/tape_.
3. A template PV (probably paused), for example, `TEMPLATE:PV` is
   added to the system with one of its stores pointing to
   _/arch/tape_.
4. For all data retrieval requests, this template PV is specified as
   the value of the `retiredPVTemplate` argument. For example,
   _<http://archiver.slac.stanford.edu/retrieval/data/getData.json?>`pv=LEGACY:PV`_&from=...&`retiredPVTemplate=TEMPLATE:PV`.
5. Because the archiver does not find a PVTypeInfo for `LEGACY:PV`,
   it uses the PVTypeInfo for `TEMPLATE:PV` to determine data stores
   for `LEGACY:PV`.
6. The data in _/arch/tape_ is used to fulfil the data retrieval
   request.
7. Once the user is done with the data for `LEGACY:PV`, the data in
   _/arch/tape_ can be deleted.

`timeranges`
: Get data for a sequence of time ranges. Time ranges are specified as
a comma-separated list of ISO 8601 strings.

`donotchunk`
: Use this to skip HTTP chunking of the response. This is meant for
clients that do not understand chunked responses.

`ca_count`
: This is passed on to an external ChannelArchiver as the value of the
_count_ parameter in the _archiver.values_ XMLRPC call. This limits
the number of samples returned from the ChannelArchiver; if
unspecified, this defaults to 100000. If this is too large, you may
see timeouts from the ChannelArchiver.

`ca_how`
: This is passed on to an external ChannelArchiver as the value of the
_how_ parameter in the _archiver.values_ XMLRPC call. This defaults
to 0; that is, by default, we ask for raw data.

## Response format

The response typically contains

1. `seconds` - This is the Java epoch seconds of the EPICS record
   processing timestamp. The times are in UTC; so any conversion to
   local time needs to happen at the client.
2. `nanos` - This is the nano second value of the EPICS record
   processing timestamp.
3. Other elements - This set includes the value, status, severity and
   many other optional fields stored by the appliance.

## Point-in-time retrieval (Save/Restore API)

The archiver also exposes a separate endpoint for fetching the value of
multiple PVs as of a single point in time. This is primarily aimed at
save/restore applications. `POST` a JSON list of PV names to
`http://archiver.slac.stanford.edu/retrieval/data/getDataAtTime?at=2018-10-19T15:22:37.000-07:00&includeProxies=true`
where

1. `at` - This specifies the point in time in ISO 8601 format.
2. `includeProxies` - Optional; set this to true if you want to fetch
   data from external archiver appliances as part of this call. This
   defaults to false, so, by default, we do not proxy external
   appliances for this API call. As of now, we also do not support
   Channel Archiver integration for this API call.
3. `searchPeriod` - Optional argument to control how far back in time to go to find data.
   Defaults to about a month. EAA uses a chunked data store; this argument determines
   which chunks to include in the search. The syntax is as specified in `java.time.Period.parse`,
   for example, to specify a year, use `P365D`.

The response is a JSON dict of dicts with the name of the PV as the key.
For example, here's a call asking for the value of a few PVs as of
`2018-10-22T10:40:00.000-07:00`

```bash
$ curl -H "Content-Type: application/json"  -XPOST -s "http://localhost:17665/retrieval/data/getDataAtTime?at=2018-10-22T10:40:00.000-07:00&includeProxies=true" -d '["VPIO:IN20:111:VRAW", "ROOM:LI30:1:OUTSIDE_TEMP", "YAGS:UND1:1005:Y_BM_CTR", "A_nonexistent_pv" ]'
{
    "ROOM:LI30:1:OUTSIDE_TEMP": {
        "nanos": 823158037,
        "secs": 1540229999,
        "severity": 0,
        "status": 0,
        "val": 60.358551025390625
    },
    "VPIO:IN20:111:VRAW": {
        "nanos": 754373158,
        "secs": 1540229999,
        "severity": 0,
        "status": 0,
        "val": 5.529228687286377
    },
    "YAGS:UND1:1005:Y_BM_CTR": {
        "nanos": 164648807,
        "secs": 1537710595,
        "severity": 0,
        "status": 0,
        "val": 0.008066000000000002
    }
}
```
