# Configuration

## Appliances XML

The `appliances.xml` is a file that lists all the appliances in a
cluster of archiver appliance. While it is not necessary to point to the
same physical file, the contents are expected to be identical across all
appliances in the cluster. The details of the file are outlined in the
[ConfigService](../_static/javadoc/org/epics/archiverappliance/config/ConfigService.html#ARCHAPPL_APPLIANCES){.external}
javadoc. A sample `appliances.xml` with two appliances looks like

```xml
<appliances>
   <appliance>
     <identity>appliance0</identity>
     <cluster_inetport>archappl0.slac.stanford.edu:16670</cluster_inetport>
     <mgmt_url>http://archappl0.slac.stanford.edu:17665/mgmt/bpl</mgmt_url>
     <engine_url>http://archappl0.slac.stanford.edu:17666/engine/bpl</engine_url>
     <etl_url>http://archappl0.slac.stanford.edu:17667/etl/bpl</etl_url>
     <retrieval_url>http://archappl0.slac.stanford.edu:17668/retrieval/bpl</retrieval_url>
     <data_retrieval_url>http://archproxy.slac.stanford.edu/archiver/retrieval</data_retrieval_url>
   </appliance>
   <appliance>
     <identity>appliance1</identity>
     <cluster_inetport>archappl1.slac.stanford.edu:16670</cluster_inetport>
     <mgmt_url>http://archappl1.slac.stanford.edu:17665/mgmt/bpl</mgmt_url>
     <engine_url>http://archappl1.slac.stanford.edu:17666/engine/bpl</engine_url>
     <etl_url>http://archappl1.slac.stanford.edu:17667/etl/bpl</etl_url>
     <retrieval_url>http://archappl1.slac.stanford.edu:17668/retrieval/bpl</retrieval_url>
     <data_retrieval_url>http://archproxy.slac.stanford.edu/archiver/retrieval</data_retrieval_url>
   </appliance>
 </appliances>
```

- The archiver appliance looks at the environment variable
  `ARCHAPPL_APPLIANCES` for the location of the `appliances.xml` file.
  Use an export statement like so

  ```bash
  export ARCHAPPL_APPLIANCES=/nfs/epics/archiver/production_appliances.xml
  ```

  to set the location of the `appliances.xml` file.

- The `appliances.xml` has one `<appliance>` section per appliance.
  Please only define those appliances that are currently in
  production. Certain BPL, most importantly, the `/archivePV` BPL,
  are suspended until all the appliances defined in the
  `appliances.xml` have started up and registered their PVs in the
  cluster.

- The `identity` for each appliance is unique to each appliance. For
  example, the string `appliance0` serves to uniquely identify the
  archiver appliance on the machine `archappl0.slac.stanford.edu`.

- The `cluster_inetport` is the `TCPIP address:port` combination that
  is used for inter-appliance communication. There is a check made to
  ensure that the hostname portion of the `cluster_inetport` is either
  `localhost` or the same as that obtained from a call to
  `InetAddress.getLocalHost().getCanonicalHostName()` which typically
  returns the fully qualified domain name (FQDN). The intent here is
  to prevent multiple appliances starting up with the same appliance
  identity (a situation that could potentially lead to data loss).

  1. For a cluster to function correctly, any member `A` of a cluster
     should be able to communicate with any member `B` of a cluster
     using `B`'s `cluster_inetport` as defined in the
     `appliances.xml`.
  2. Obviously, `localhost` should be used for the `cluster_inetport`
     only if you have a cluster with only one appliance. Even in this
     case, it's probably more future-proof to use the FQDN.

- For the ports, it is convenient if

  - The port specified in the `cluster_inetport` is the same on all
    machines. This is the port on which the appliances talk to each
    other.
  - The `mgmt_url` has the smallest port number amongst all the web
    apps.
  - The port numbers for the other three web apps increment in the
    order shown above.

    Again, there is no requirement that this be the case. If you follow
    this convention, you can use the standard deployment scripts with
    minimal modification.

- There are two URL's for the `retrieval` webapp.

  1. The `retrieval_url` is the URL used by the `mgmt` webapp to talk
     to the `retrieval` webapp.
  2. The `data_retrieval_url` is used by archive data retrieval
     clients to talk to the cluster. In this case, we are pointing
     all clients to a single load-balancer on
     `archproxy.slac.stanford.edu` on port 80. One can use the
     [mod_proxy_balancer](http://httpd.apache.org/docs/2.4/mod/mod_proxy_balancer.html)
     of Apache to load-balance among any of the appliances in the
     cluster.

     ![Using Apache HTTP on `archiver` to load balance data retrieval between `appliance0` and `appliance1`.](../images/ApacheasLB.png)

     - Note there are also other load-balancing solutions available
       that load-balance the HTTP protocol that may be more
       appropriate for your installation.
     - Also, note that Apache+Tomcat can also use a binary protocol
       called `AJP` for load-balancing between Apache and Tomcat.
       For this software, we should use simple HTTP; this workflow
       does not entail the additional complexity of the `AJP`
       protocol.
