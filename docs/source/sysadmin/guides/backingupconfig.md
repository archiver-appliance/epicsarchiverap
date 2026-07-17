## Backing up your config databases

Each appliance has a config database; typically a MySQL database. This
config database contains the archiving configuration; that is, how each
PV is being archived and where the data is stored. So, it\'s good policy
to back this database up periodically. For example, one can do a daily
backup using `mysqldump`; this should be more than adequate.

```bash
mysqldump -u userid -p password  archappl > /path/to/backupfile
```

It is also good practice to validate the backups every so often. Most
labs have some scripts that perform some basic validation of these
backups. Please ask if you need more info.

Note that because the config database contains only configuration, the
database itself should see very little traffic. In fact, if you are not
adding any PV\'s or altering the configuration in any way, you should
not have any traffic on the database at all. Each appliance assumes that
it has complete ownership of its config database; so it makes sense to
have the database as part of the appliance itself. However, it is good
practice to store the backup file elsewhere; perhaps in a location that
is redundant and is itself backed to tape.
