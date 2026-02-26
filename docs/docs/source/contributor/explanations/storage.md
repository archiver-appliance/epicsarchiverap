## Storage

Out of the box, the following storage technologies/plugins are
supported.

[PlainStoragePlugin](../_static/javadoc/edu/stanford/slac/archiverappliance/plain/PlainStoragePlugin.html){.external}
: This plugin serializes samples using either Google\'s
[ProtocolBuffers](https://developers.google.com/protocol-buffers) (PB) or
[Apache Parquet](parquet) and stores data in chunks. Each chunk has a
well defined key and stores data for one PV for a well defined time
duration (for example, a month). Using Java
[NIO2](http://docs.oracle.com/javase/7/docs/api/java/nio/file/package-summary.html),
one can store each chunk in 1. A file per chunk resulting in a file per PV per time partition. 2. A zip file entry in a `.zip` file per chunk resulting in a
`.zip` file per PV. 3. This can be extended to use other storage technologies for which
a NIO2 provider is available (for example, [Amazon
S3](https://github.com/Upplication/Amazon-S3-FileSystem-NIO2), a
database BLOB per chunk or a key/value pair per chunk in any
key/value store).

    :::{note}
    By default, the PlainStoragePlugin maps PV names to keys using a
    simple algorithm that relies on the presence of a good PV naming
    convention. To use your own mapping scheme, see the [Key Mapping](../sysadmin/customization#key_mapping) section in the
    customization guide.
    :::

To add support for other storage technologies - see the [customization
guide](../sysadmin/customization) for details.
