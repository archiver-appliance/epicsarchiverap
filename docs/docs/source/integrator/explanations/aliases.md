## EPICS aliases

When adding a PV with an alias to the archiver, the archiver uses the
`NAME/NAME$` fields of the PV to determine the _real_ name. The PV is
archived under the _real_ name and an entry is added to the `PVAliases`
table with the alias name. Data retrieval and management are then
supported under both names.

For aliased PVs, the PVDetails page will show a line indicating that
this PV is an alias for the _real_ PV. In the mgmt webapp's `arch.log`,
there will be an entry like: _Aborting archive request for pv
ABC:DEF:ALIAS Reason: Aborting this pv ABC:DEF:ALIAS (which is an
alias) and using the real name ABC:DEF:REAL instead._
