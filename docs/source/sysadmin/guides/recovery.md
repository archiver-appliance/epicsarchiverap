## Recovering from a lost appliance

In case you lose an appliance, you can use the backup of the config
database to restore the appliance configuration onto a new machine.
Simply go thru the install procedure and create an appliance with the
same identity as the machine that was lost. This should yield an
appliance with an empty config database; you can import the
configuration into this empty database using

```bash
mysql -u userid -p password  archappl < /path/to/backupfile
```

Restarting the appliance after this should pick up the imported
configuration. As the JVM can cache DNS lookups, giving your replacement
appliance the same IP address as the one that was lost should also help
if you have a cluster of machines.
