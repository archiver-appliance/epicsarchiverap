# Administration Guide

## Scripting

For bulk operations, most administrators will almost find the scripting
interface useful as well. All actions in the UI (in addition to a few
that are not exposed in the UI) are accessible from within scripts -
please see the [details](../developer/details.md#scripting) page for more info.
Click [here](../developer/mgmt_scriptables) for a list of
business logic accessible thru scripting. There are also many scripting
samples in the _`tomcat_mgmt`_`/webapps/mgmt/ui/help/samples/` folder.

## Monitoring

Here are some aspects of the EPICS archiver appliance that should be
monitored

Logs
: Monitor the logs periodically for Exceptions,
OutOfMemory and FATAL error messages. You can use a variation of
these commands

    ```bash
    find /arch/tomcats -wholename '*/logs/*' -exec grep -l xception {} \;
    find /arch/tomcats -wholename '*/logs/*' -exec grep -l FATAL {} \;
    find /arch/tomcats -wholename '*/logs/*' -exec grep -l OutOfMemoryError {} \;
    ```

    While exceptions in the retrieval and mgmt components could
    potentially be from user errors, any exceptions/FATAL messages in
    the ETL/Engine components should immediately be investigated.

Disk free space
: Monitor the disk free space in each of
your stores (raising alarms if disk usage increases about a certain
limit).

Connected PVs
: You can use the `getApplianceMetrics` BPL
(see `samples/checkConnectedPVs.py`) to monitor the number of
currently disconnected PVs. You can then send an email notification
to the system administrators if this is greater than a certain
percentage or absolute number.

Type changes
: You can use the
`/getPVsByDroppedEventsTypeChange` BPL (see
`samples/checkTypeChangedPVs.py`) to watch for any PV\'s that have
changed type. If a PV changes type, the EPICS archiver appliance
will suspend archiving this PV until the situation is manually
resolved.

1.  You can rename the PV to a new name.

    1.  Pause the PV under the current name.
    2.  Rename the PV to a new name using the `/renamePV` BPL or the
        UI
    3.  Delete the PV under the current name.
    4.  Re-archive under the current name.

            This should now archive the PV using the new type; however,
            requests for the older data (which is of the older type) will
            have to made using the older name.

2.  The EPICS archiver appliance has some support for converting
    data from one type to the other. This is not available in all
    cases but you should be able to convert most scalars.

    1. Pause the PV
    2. If needed, consolidate and make a backup of the data for
       this PV.
    3. Convert to the new type using the `/changeTypeForPV` BPL
    4. Resume the PV (if the conversion process succeeds)

    The `/changeTypeForPV` alters the data that has already been
    archived; so you may want to make a backup first.

Maintaining a clean system
: Monitoring connected PVs (see
above) is made significantly easier if you maintain a clean system.
One strategy that can be used is to pause PV\'s that have been
disconnected for more than a certain time. The
`/getCurrentlyDisconnectedPVs` returns a list of currently
disconnected PVs and some notion of when the connection to this PV
was lost.

- You can (perhaps automatically) pause PVs that have been
  disconnected for more than a certain period of time.
- You can (perhaps automatically) resume PVs that have been paused
  (obtained using the `/getPausedPVsReport`) but are now alive.
- Optionally, you can potentially delete PVs that have been paused
  for some time and are still not alive.

## Backing up your config databases

Each appliance has a config database; typically a MySQL database. This
config database contains the archiving configuration; that is, how each
PV is being archived and where the data is stored. So, it\'s good policy
to back this database up periodically. For example, one can do a daily
backup using `mysqldump`; this should be more than adequate.

```bash
mysqldump -u userid -p password  archappl > /path/to/backupfile
```

It is also good practice to validate the backups every so often. Most
labs have some scripts that perform some basic validation of these
backups. Please ask if you need more info.

Note that because the config database contains only configuration, the
database itself should see very little traffic. In fact, if you are not
adding any PV\'s or altering the configuration in any way, you should
not have any traffic on the database at all. Each appliance assumes that
it has complete ownership of its config database; so it makes sense to
have the database as part of the appliance itself. However, it is good
practice to store the backup file elsewhere; perhaps in a location that
is redundant and is itself backed to tape.

## Recovering from a lost appliance

In case you lose an appliance, you can use the backup of the config
database to restore the appliance configuration onto a new machine.
Simply go thru the install procedure and create an appliance with the
same identity as the machine that was lost. This should yield an
appliance with an empty config database; you can import the
configuration into this empty database using

```bash
mysql -u userid -p password  archappl < /path/to/backupfile
```

Restarting the appliance after this should pick up the imported
configuration. As the JVM can cache DNS lookups, giving your replacement
appliance the same IP address as the one that was lost should also help
if you have a cluster of machines.

## Inspecting the Channel Access ( also PVAccess ) protocol

To troubleshoot certain issues in production, it is often more practical
to inspect the wire protocol. For example, to see if the archivers are
issuing search requests to the IOC when looking into connectivity
issues, it\'s faster to get a packet capture from production and then
inspect the same elsewhere. Michael Davidsaver maintains a [Wireshark LUA plugin](https://github.com/mdavidsaver/cashark/) that can understand
Channel Access ( and PVAccess ). Here\'s an example of this process

- First, take a packet capture using Wireshark or tcpdump. For
  example, you can constrain tcpdump to capture packets on the network
  interface `em1` between the archiver appliance and the IOC using
  something like so

  ```bash
  /usr/sbin/tcpdump -i em1 'host ioc_or_gateway_hostname and appliance_hostname' -w /localdisk/captured_packets
  ```

- The file `/localdisk/captured_packets` contains the packet capture
  and can be copied over to a dev box and inspected at leisure.
- Wireshark has a very comprehensive GUI and can be used to load the
  packet capture. Alternatively, one can also use the command line
  variant of Wireshark called `tshark` like so

  ```bash
  tshark -X lua_script:ca.lua -r captured_packets 2>&1 | less
  ```

- Recent version of the EPICS archiver appliance display the Channel
  Access `CID - Client ID`, `SID - Server ID` and `Subscription ID` in
  the PV details page.
- Pausing and resuming the PV during packet capture will also enable
  the `cashark` plugin to track the life cycle of the Channel Access
  channel.

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
