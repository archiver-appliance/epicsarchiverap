## Temporarily suspend using a store

If one of your data stores uses storage from elsewhere and you expect
an outage or disruption of connectivity, you can
temporarily suspend using the store for retrieval requests.
This is especially useful if you mount the remote location using NFS
which can sometimes suspend threads forever during
periods of network disruptions ( see the hard/soft/intr options for NFS clients).
For example, if you have a data store named `LTS`, you can set the
`SKIP_LTS_FOR_RETRIEVAL` named flag to `true` and this will stop adding the `LTS`
to retrieval requests ( both `getData` and `getDataAtTime` ). Once the outage
is over, you can turn `LTS` back on by setting the `SKIP_LTS_FOR_RETRIEVAL`
named flag to `false`. Named flags are `false` by default; so restarting the
appliances should also accomplish the same.

Use a named flag based on the storage plugin's name.
For example, if the name is LTS, the named flag
SKIP_LTS_FOR_RETRIEVAL can used to temporarily turn off
using the LTS for retrieval.

Similarly, use a named flag `SKIP_LTS_FOR_ETL` to temporarily suspend
ETL into the `LTS` data store.
