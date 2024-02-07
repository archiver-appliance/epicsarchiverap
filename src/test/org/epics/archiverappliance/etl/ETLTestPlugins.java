package org.epics.archiverappliance.etl;

import edu.stanford.slac.archiverappliance.plain.PlainStoragePlugin;

import java.util.List;

public record ETLTestPlugins(PlainStoragePlugin src, PlainStoragePlugin dest) {
    public static List<ETLTestPlugins> generatePlugins() {

        return List.of(new ETLTestPlugins(new PlainStoragePlugin(), new PlainStoragePlugin()));
    }

    public String pvNamePrefix() {
        return String.format("DEST%sSRC%s", this.dest.getPluginIdentifier(), this.src.getPluginIdentifier());
    }
}
