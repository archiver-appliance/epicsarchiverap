# References

```{toctree}
:maxdepth: 2

Storage Plugins <storage_plugins>
Screenshots <screenshots>

[Management API]
[Settings file]
[Appliances configuration]
[Docker compose]
[Scripts list]
```

## Environment variables

The key environment variables used to configure the archiver appliance are:

| Variable                       | Description                                                                                                   | Default                                                       |
| ------------------------------ | ------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------- |
| `ARCHAPPL_APPLIANCES`          | Path to `appliances.xml` listing all appliances in the cluster. Must be identical across all cluster members. | `appliances.xml`                                              |
| `ARCHAPPL_POLICIES`            | Full path to your `policies.py` file.                                                                         | `policies.py` in `WEB-INF/classes`                            |
| `ARCHAPPL_PROPERTIES_FILENAME` | Full path to `archappl.properties` (misc configuration bucket).                                               | `archappl.properties` in `WEB-INF/classes`                    |
| `ARCHAPPL_MYIDENTITY`          | Identity of this appliance; must match the `identity` element in `appliances.xml`.                            | Result of `InetAddress.getLocalHost().getCanonicalHostName()` |

In addition, the configuration for each PV is stored in the `PVTypeInfo`
table in the MySQL configuration database specific to each appliance. The
connection pool for the database is configured in Tomcat's
`conf/context.xml`.
