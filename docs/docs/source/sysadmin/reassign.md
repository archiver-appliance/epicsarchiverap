# Dynamic (re)-assignment of PVs to appliances
The EAA primarily uses clustering to support archiving millions of PVs.
When a PV `PV1` is added to EAA, the `mgmt` component in EAA assigns the PV to one of the appliances in the cluster. 
That appliance then establishes `ca/pva_monitors` to the PV and starts writing samples into a sequence of stores.
In the following example, if `PV1` is assigned to appliance0, then the data for `PV1` is stored in `STS_0`, `MTS_0`, `LTS_0`
with the ETL on appliance0 moving data from `STS_0` &#x2192; `MTS_0` &#x2192; `LTS_0`.
```{mermaid}
flowchart TB
  subgraph appliance0
    direction LR
    STS_0 --> MTS_0 --> LTS_0
  end
  subgraph appliance1
    direction LR
    STS_1 --> MTS_1 --> LTS_1
  end
  appliance0 <--JSON/HTTP--> appliance1
```
The EAA supports a `reshard`ing operation that can move the PV `PV1` from appliance0 to appliance1.
This is a heavyweight operation as it involves first copying the data from `STS_0`, `MTS_0`, `LTS_0`
to `STS_1`, `MTS_1`, `LTS_1`, moving the PV to appliance1 and then finally deleteing the data 
from `STS_0`, `MTS_0`, `LTS_0`. The reshard operation can take several seconds/minutes to complete.

The `reshard` BPL is a perfectly adequate approach for installations where the number of appliances in the cluster 
and the set of PV's being archived is reasonably static. In more modern cloud-centric environments, 
devops engineers expect to be able to change the number of appliances in the cluster 
by changing a single attribute in a YAML file. A more lightweight operation for moving PV's from one
appliance to another would help support this. 

However, such a lightweight `reassign` operation
requires careful setup of the data stores a-priori as the EAA is a stateful entity. This setup involves
sharing some of the later data stores among all the appliances in the cluster and applying
`consolidateOnShutdown` on the earlier data stores that are specific to the appliance.

For example, in a typical installation, the STS and MTS are local on the appliance hardware 
while the LTS and XLTS are on remote network storage. Often the STS is a RAMDisk, and thus
has a `consolidateOnShutdown` attribute on the data store to move the data off the STS onto the MTS
when the appliance is being gracefully shutdown or the PV is being paused. 
```{mermaid}
flowchart LR
  subgraph Appliance_Hardware
    direction LR
    STS["STS with consolidateOnShutdown"]
    STS --> MTS
  end
  subgraph Network_Storage
    direction LR
    LTS --> XLTS
  end
  Appliance_Hardware --> Network_Storage
```
One can configure the data stores such that the LTS and XLTS are shared between the appliances.
That is, both appliance0 and appliance1 write to the exact same location on the LTS and XLTS.
But as the PVs are distributed between the appliances on a one-to-one basis, there should be no conflicts
when writing data as no two appliances will archive the same PV.
```{mermaid}
flowchart TB
  subgraph appliance0
    direction LR
    STS_0
    MTS_0
    STS_0 --> MTS_0
  end
  subgraph appliance1
    direction LR
    STS_1
    MTS_1
    STS_1 --> MTS_1
  end
  subgraph Network_Storage
    direction LR
    LTS --> XLTS
  end
  appliance0 --> Network_Storage
  appliance1 --> Network_Storage
```
Note that there may be some performance impact from a shared LTS/XLTS as data files from both appliance0 and appliance1 will now
be stored in the same folders on LTS/XLTS. And since retrieval typically involves doing an `ls` in a folder with many thousands of files,
increasing the number of files in your LTS/XLTS folder may have performance implications and some impact on the metadata servers for
your file system.

However, in such a configuration with a shared LTS/XLTS, if we configure both the STS and MTS to have `consolidateOnShutdown`,
then, to move a PV from appliance0 to appliance1
- We pause the PV on appliance0. This should then flush all the data for the PV into the shared LTS
- We change the appliance assignment for the PV in the cluster from appliance0 to appliance1.
- We resume the PV on appliance1. Since the LTS is shared, appliance1 should now be able to access 
the data for the PV from the shared LTS and there is no loss of data.
```{mermaid}
flowchart LR
  subgraph Appliance_Specific_Storage
    direction LR
    STS["STS with consolidateOnShutdown"]
    MTS["MTS with consolidateOnShutdown"]
    STS --> MTS
  end
  subgraph Shared_Network_Storage
    direction LR
    LTS --> XLTS
  end
  Appliance_Specific_Storage --> Shared_Network_Storage
```
With recent releases, the EAA appliances now react to changes in `PVTypeInfo`.
So these multiple steps for PV reassignment can be accomplished by simply changing 
the appliance assignment for the PV in the cluster/`PVTypeInfo` from appliance0 to appliance1.
And this is what the `reassignAppliance` BPL does.

Executing the `reassignAppliance` BPL in an installation that is not configured to support it can
result in data loss with data for a PV being stranded across multiple appliances in the cluster. Therefore, this BPL is disabled
by default. To enable this for your installation, please use the `org.epics.archiverappliance.mgmt.bpl.ReassignAppliance`
property in your `archappl.properties`. 
