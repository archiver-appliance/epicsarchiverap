# Archiver Appliance documentation

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

Last published on {sub-ref}`today`. This project is part of the
[AccelUtils](http://accelutils.sourceforge.net/) collaboration.

```{toctree}
:caption: Operator documentation
:maxdepth: 3
:glob:

operator/tutorials/index
operator/guides/index
operator/explanations/index
operator/references/index
```

```{toctree}
:caption: Integrator documentation
:maxdepth: 3
:glob:

integrator/tutorials/index
integrator/guides/index
integrator/explanations/index
integrator/references/index
```

```{toctree}
:caption: System admin documentation
:maxdepth: 3
:glob:

sysadmin/tutorials/index
sysadmin/guides/index
sysadmin/explanations/index
sysadmin/references/index
```

```{toctree}
:caption: Developer documentation
:maxdepth: 3
:glob:

developer/tutorials/index
developer/guides/index
developer/explanations/index
developer/references/index
```

```{toctree}
:caption: Contributor documentation
:maxdepth: 3
:glob:

contributor/tutorials/index
contributor/guides/index
contributor/explanations/index
contributor/references/index
```

```{toctree}
:caption: Miscellaneous documentation
:maxdepth: 3
:glob:
FAQ <faq>
```
