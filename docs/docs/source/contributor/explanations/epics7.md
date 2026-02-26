## EPICS 7

The archiver appliance has built in support for EPICS 7 and archiving
PV\'s over PVAccess. NTScalars and NTScalarArrays are stored as their
channel access counterparts. For example, `PVDouble`\'s will be stored
as `DBR_SCALAR_DOUBLE`\'s. This makes it possible to use standard
archive viewers to view NTScalars and NTScalarArrays archived thru
PVAccess. Other PVData types are stored as a bunch of bits using
PVAccess serialization. While this is probably not the most efficient,
it does allow for archiving of arbitrary structured data. There is
support for retrieving of structured data using the `RAW` and `JSON`
formats.
