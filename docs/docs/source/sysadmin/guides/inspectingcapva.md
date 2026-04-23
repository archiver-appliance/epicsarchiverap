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
