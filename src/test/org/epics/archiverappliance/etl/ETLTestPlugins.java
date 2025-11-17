package org.epics.archiverappliance.etl;

import edu.stanford.slac.archiverappliance.plain.PlainStoragePlugin;
import edu.stanford.slac.archiverappliance.plain.PlainStorageType;
import org.junit.jupiter.params.provider.Arguments;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public record ETLTestPlugins(PlainStoragePlugin src, PlainStoragePlugin dest) {
    public static List<ETLTestPlugins> generatePlugins() {

        return generatePlainStorageType().stream()
                .map(fPair -> new ETLTestPlugins(new PlainStoragePlugin(fPair[0]), new PlainStoragePlugin(fPair[1])))
                .toList();
    }

    public static List<PlainStorageType[]> generatePlainStorageType() {
        List<PlainStorageType[]> plainStorageTypes = new ArrayList<>();
        plainStorageTypes.add(new PlainStorageType[] {PlainStorageType.PB, PlainStorageType.PB});
        plainStorageTypes.add(new PlainStorageType[] {PlainStorageType.PB, PlainStorageType.PARQUET});
        plainStorageTypes.add(new PlainStorageType[] {PlainStorageType.PARQUET, PlainStorageType.PB});
        plainStorageTypes.add(new PlainStorageType[] {PlainStorageType.PARQUET, PlainStorageType.PARQUET});

        return plainStorageTypes;
    }

    public static Stream<Arguments> providePlainStorageTypeArguments() {
        return ETLTestPlugins.generatePlainStorageType().stream().map(fPair -> Arguments.of(fPair[0], fPair[1]));
    }

    public String pvNamePrefix() {
        return String.format("DEST%sSRC%s", this.dest.pluginIdentifier(), this.src.pluginIdentifier());
    }
}
