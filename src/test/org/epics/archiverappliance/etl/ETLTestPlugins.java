package org.epics.archiverappliance.etl;

import edu.stanford.slac.archiverappliance.plain.PlainStoragePlugin;
import edu.stanford.slac.archiverappliance.plain.PlainStorageType;

import java.util.List;

public record ETLTestPlugins(PlainStoragePlugin src, PlainStoragePlugin dest) {
    public static List<ETLTestPlugins> generatePlugins() {

        return List.of(new ETLTestPlugins(
                new PlainStoragePlugin(PlainStorageType.PB), new PlainStoragePlugin(PlainStorageType.PB)));
    }

    public String pvNamePrefix() {
        return String.format("DEST%sSRC%s", this.dest.pluginIdentifier(), this.src.pluginIdentifier());
    }
}
