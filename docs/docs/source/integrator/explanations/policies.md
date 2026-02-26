## Policies

All of the various configurations can get quite tricky for end users to
navigate. Rather than expose all of this variation to the end users and
to provide a simple interface to end users, the archiver appliance uses
[policies](../_static/javadoc/org/epics/archiverappliance/mgmt/policy/package-summary.html){.external}.
Policies are Python scripts that make these decisions on behalf of the
users. Policies are site-specific and identical across all appliances in
the cluster. When a user requests a new PV to be archived, the archiver
appliance samples the PV to determine event rate, storage rate and other
parameters. In addition, various fields of the PV like .NAME, .ADEL,
.MDEL, .RTYP etc are also obtained. These are passed to the policies
python script which then has some simple code to configure the detailed
archival parameters. The archiver appliance executes the `policies.py`
python script using an embedded [jython](http://www.jython.org/)
interpreter. Policies allow system administrators to support a wide
variety of configurations that are more appropriate to their
infrastructure without exposing the details to their users.
