package edu.stanford.slac.archiverappliance.plain;

import org.junit.jupiter.params.provider.Arguments;

import java.util.stream.Stream;

public class PlainStoragePluginOptions {
    public static Stream<Arguments> provideStoragePlugins() {
        return Stream.of(
                Arguments.of(new PlainStoragePlugin(PlainStorageType.PB)),
                Arguments.of(new PlainStoragePlugin(PlainStorageType.PARQUET)));
    }
}
