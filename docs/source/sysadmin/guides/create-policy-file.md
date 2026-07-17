# Create your policies file

The EPICS archiver appliance ships with a sample
[`policies.py`](customization.md#policies) (from the `tests` site)
that creates a three stage storage environment. These are

1. **STS** - A datastore that uses the
   [PlainStoragePlugin](../_static/javadoc/edu/stanford/slac/archiverappliance/plain/PlainStoragePlugin.html)
   to store data in a folder specified by the environment variable
   `ARCHAPPL_SHORT_TERM_FOLDER` at the granularity of an hour.
2. **MTS** - A datastore that uses the
   [PlainStoragePlugin](../_static/javadoc/edu/stanford/slac/archiverappliance/plain/PlainStoragePlugin.html)
   to store data in a folder specified by the environment variable
   `ARCHAPPL_MEDIUM_TERM_FOLDER` at the granularity of a day.
3. **LTS** - A datastore that uses the
   [PlainStoragePlugin](../_static/javadoc/edu/stanford/slac/archiverappliance/plain/PlainStoragePlugin.html)
   to store data in a folder specified by the environment variable
   `ARCHAPPL_LONG_TERM_FOLDER` at the granularity of an year.

If you are using the generic build and would like to point to a
different `policies.py` file, you can use the `ARCHAPPL_POLICIES`
environment variable, like so.

```bash
export ARCHAPPL_POLICIES=/nfs/epics/archiver/production_policies.py
```

On the other hand, if you are using a site specific build, you can
bundle your site-specific `policies.py` as part of the `mgmt WAR` during
the site specific build. Just add your `policies.py` to the source code
repository under `src/sitespecific/YOUR_SITE/classpathfiles` and build
the war by setting the `ARCHAPPL_SITEID` during the build using
something like `export ARCHAPPL_SITEID=YOUR_SITE`. In this case, you do
not need to specify the `ARCHAPPL_POLICIES` environment variable.

For more detail on writing policies, including Jython scripting examples,
see [Customization](customization.md#policies).

## Setting up storage folders

The default `policies.py` uses three environment variables to locate
storage. A useful way to do this is to create a folder called `/arch`
and then create soft links in this folder to the actual physical
locations. For example,

```bash
$ ls -ltra /arch
lrwxrwxrwx    1 archappl archappl      8 Jun 21  2013 sts -> /dev/shm
lrwxrwxrwx    1 archappl archappl      4 Jun 21  2013 mts -> data
lrwxrwxrwx    1 archappl archappl     40 Feb 12  2014 lts -> /nfs/site/archappl/archappl01
drwxr-xr-x  195 archappl archappl    4096 Oct 15 15:05 data
```

Then set environment variables in the startup script that point to the
locations within `/arch`:

```bash
export ARCHAPPL_SHORT_TERM_FOLDER=/arch/sts/ArchiverStore
export ARCHAPPL_MEDIUM_TERM_FOLDER=/arch/mts/ArchiverStore
export ARCHAPPL_LONG_TERM_FOLDER=/arch/lts/ArchiverStore
```
