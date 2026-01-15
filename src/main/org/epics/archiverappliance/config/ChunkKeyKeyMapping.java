package org.epics.archiverappliance.config;

import org.epics.archiverappliance.config.exception.ConfigException;

public class ChunkKeyKeyMapping implements PVNameToKeyMapping {
    private ConfigService configService;
    private char terminatorChar = 0;

    @Override
    public void initialize(ConfigService configService) throws ConfigException {
        this.configService = configService;
    }

    public ChunkKeyKeyMapping(ConfigService configService) {
        this.configService = configService;
    }

    public ChunkKeyKeyMapping(ConfigService configService, char terminatorChar) {
        this.configService = configService;
        this.terminatorChar = terminatorChar;
    }

    @Override
    public String convertPVNameToKey(String pvName) {
        String chunkKey;
        PVTypeInfo typeInfo = this.configService.getTypeInfoForPV(pvName);
        if (typeInfo == null) {
            // Mostly in the unit tests.
            chunkKey = this.configService.getPVNameToKeyConverter().convertPVNameToKey(pvName);
        } else {
            chunkKey = typeInfo.getChunkKey();
            if (chunkKey == null || chunkKey.isEmpty()) {
                chunkKey = this.configService.getPVNameToKeyConverter().convertPVNameToKey(pvName);
            }
        }

        if (terminatorChar != 0) {
            chunkKey = chunkKey.replace(chunkKey.charAt(chunkKey.length() - 1), terminatorChar);
        }
        return chunkKey;
    }

    @Override
    public String[] breakIntoParts(String pvName) {
        return this.configService.getPVNameToKeyConverter().breakIntoParts(pvName);
    }

    @Override
    public PVNameToKeyMapping overrideTerminator(char terminator) {
        ChunkKeyKeyMapping chunkKeyKeyMapping = new ChunkKeyKeyMapping(this.configService, terminator);
        return chunkKeyKeyMapping;
    }
}
