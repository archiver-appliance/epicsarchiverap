# Save/Restore API

The EPICS Archiver Appliance has a separate API targeted at getting the
value of several PV's as of a point in time. This is primarily aimed at
save/restore applications where the archiver is often used as quality
control. To get data for multiple PV's as of a point in time, `POST` a
JSON list of PV names to
`http://archiver.slac.stanford.edu/retrieval/data/getDataAtTime?at=2018-10-19T15:22:37.000-07:00&includeProxies=true`
where

1. `at` - This specifies the point in time in ISO8601 format.
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
For example, here's a call asking for the value of a few PV's as of
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
