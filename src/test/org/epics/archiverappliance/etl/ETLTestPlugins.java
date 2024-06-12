package org.epics.archiverappliance.etl;

import edu.stanford.slac.archiverappliance.PlainPB.PlainPBStoragePlugin;

import java.util.List;

public record ETLTestPlugins(PlainPBStoragePlugin src, PlainPBStoragePlugin dest) {
    public static List<ETLTestPlugins> generatePlugins() {

        return List.of(new ETLTestPlugins(new PlainPBStoragePlugin(), new PlainPBStoragePlugin()));
    }

    public String pvNamePrefix() {
        return String.format("DEST%sSRC%s", this.dest.pluginIdentifier(), this.src.pluginIdentifier());
    }
}
