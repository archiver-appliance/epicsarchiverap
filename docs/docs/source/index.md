# EPICS Archiver Appliance

The EPICS Archiver Appliance (EAA) is an implementation of an archiver for
[EPICS](http://www.aps.anl.gov/epics/index.php) control systems that
aims to archive millions of PVs.

At a high level, some features are

- Ability to cluster appliances and to scale by adding appliances to
  the cluster.
- Multiple stages and an inbuilt process to move data between the
  stages.
- Focus on data retrieval performance.
- Focus on zero oversight.

For a more detailed description, please see the [Details](developer/details)
page.

To get started, please see the [Quickstart](sysadmin/quickstart) guide.

Last published on {sub-ref}`today`. This project is part of the
[AccelUtils](http://accelutils.sourceforge.net/) collaboration.

```{toctree}
---
hidden:
maxdepth: 1
---
faq.md
license.md
```

```{toctree}
:hidden:
:caption: User

user/cstudio.md
user/matlab.md
user/userguide.md
user/archiveviewer.md
```

```{toctree}
:hidden:
:caption: Sysadmin

sysadmin/admin.md
sysadmin/customization.md
sysadmin/installguide.md
sysadmin/quickstart.md
sysadmin/redundancy.md
sysadmin/reassign.md
sysadmin/site_specific.md
sysadmin/sqlite.md
```

```{toctree}
:hidden:
:caption: Developer

developer/details.md
developer/developersguide.md
developer/pb_pbraw.md
developer/parquet.md
Management Scriptables <developer/mgmt_scriptables.md>
```
