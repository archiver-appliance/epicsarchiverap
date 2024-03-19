CREATE TABLE PVTypeInfo ( 
	pvName TEXT NOT NULL UNIQUE PRIMARY KEY,
	typeInfoJSON TEXT NOT NULL,
	last_modified TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TRIGGER PVTypeInfo_update AFTER UPDATE ON PVTypeInfo FOR EACH ROW
BEGIN
	UPDATE PVTypeInfo SET last_modified = datetime('now') WHERE rowid==NEW.rowid;
END;

CREATE TABLE PVAliases ( 
	pvName TEXT NOT NULL UNIQUE PRIMARY KEY,
	realName TEXT NOT NULL,
	last_modified TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TRIGGER PVAliases_update AFTER UPDATE ON PVAliases FOR EACH ROW
BEGIN
	UPDATE PVAliases SET last_modified = datetime('now') WHERE rowid==NEW.rowid;
END;

CREATE TABLE ArchivePVRequests ( 
	pvName TEXT NOT NULL PRIMARY KEY,
	userParams TEXT NOT NULL,
	last_modified TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TRIGGER ArchivePVRequests_update AFTER UPDATE ON ArchivePVRequests FOR EACH ROW
BEGIN
	UPDATE ArchivePVRequests SET last_modified = datetime('now') WHERE rowid==NEW.rowid;
END;

CREATE TABLE ExternalDataServers ( 
	serverid TEXT NOT NULL UNIQUE PRIMARY KEY,
	serverinfo TEXT NOT NULL,
	last_modified TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TRIGGER ExternalDataServers_update AFTER UPDATE ON ExternalDataServers FOR EACH ROW
BEGIN
	UPDATE ExternalDataServers SET last_modified = datetime('now') WHERE rowid==NEW.rowid;
END;
