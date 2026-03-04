## Log file locations

Most deployments have four Tomcat containers per appliance — one each
for the engine, ETL, retrieval and mgmt components. All logs are
typically sent to `arch.log` or similar files in the `logs/` folder of
each container's `CATALINA_BASE`. Log levels are controlled using a
`log4j2.xml` file in the `TOMCAT_HOME/lib` folder of each container.

## Monitoring your EPICS archiver appliance

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
