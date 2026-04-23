# Install Guide

If you want to simply test the system and quickly get going, please see
the [Quickstart](quickstart) section.

## Customize preexisting VM's

A simple way to get an installation going is to clone and customize
Martin's repos for your installation. These consist of three repos that
are needed to set up the Archiver Appliance environment.

1. [vagrant_archiver_appliance](https://github.com/mark0n/vagrant_archiver_appliance)
2. [puppet_module_archiver_appliance](https://forge.puppet.com/mark0n/epics_archiverappliance)
3. [puppet_module_epics_softioc](https://forge.puppet.com/mark0n/epics_softioc)

Simply follow the rules in the README of the first repo and the other
two repos will be pulled in automatically. The Puppet manifests are
found in puppet_module_archiver_appliance.

## Site specific installs

Han maintains a set of scripts for [site specific installs](https://github.com/jeonghanlee/epicsarchiverap-env). This is
an excellent starting off point for folks who wish to build their own
deployment bundles. This is tested against Debian/CentOS; but should be
easily extensible for other distributions.

## Using an install script

If you plan to have only one machine in the cluster, you can consider
using the `install_scripts/single_machine_install.sh` install script
that comes with the installation bundle. This install script
accommodates installations with a "standard" set of parameters and
installs the EPICS archiver appliance on one machine. In addition to the
[System requirements](../../contributor/explanations/systemreqs.md#system-requirements), the
`install_scripts/single_machine_install.sh` will ask for

1. Location of the Tomcat distribution.

2. Location of the MySQL client jar - usually a file with a name like
   `mysql-connector-java-5.1.21-bin.jar`

3. A MySQL connection string that looks like so
   `--user=archappl --password=archappl --database=archappl` that can
   be used with the MySQL client like so
   `mysql ${MYSQL_CONNECTION_STRING} -e "SHOW DATABASES"`. This implies
   that the MySQL schema has already been created using something like

   ```bash
   mysql --user=root --password=*****
   CREATE DATABASE archappl;
   GRANT ALL ON archappl.* TO 'archappl' identified by 'archappl';
   ```

The `install_scripts/single_machine_install.sh` install script creates a
couple of scripts in the deployment folder that can be customized for
your site.

1. **`sampleStartup.sh`** - This is a script in the fashion of scripts
   in `/etc/init.d` that can be used to start and stop the four Tomcat
   processes of your archiver appliance.
2. **`deployRelease.sh`** - This can be used to upgrade your
   installation to a new release of the EPICS archiver appliance. The
   `deployRelease.sh` also includes some post install hooks to deploy
   your site specific content as outlined
   [here](site_specific).

## Manual installation overview

For finer control over your installation, the steps are:

For the cluster:

1. Create an `appliances.xml` (see [Configuration](../explanations/configuration))
2. Optionally, create your `policies.py` file (see [Policies](../guides/policies))

In addition to installing the JDK and EPICS (see [System requirements](../../contributor/explanations/systemreqs.md#system-requirements)), for each appliance:

1. [Install and configure Tomcat](../guides/tomcat-setup)
2. [Install MySQL](../guides/mysql-setup)
3. Set up storage folders (see [Policies](../guides/policies#setting-up-storage-folders))
4. [Deploy the WAR files](../guides/deployment)
5. [Start the Tomcats](../guides/deployment#stopping-and-starting-the-individual-tomcats)
