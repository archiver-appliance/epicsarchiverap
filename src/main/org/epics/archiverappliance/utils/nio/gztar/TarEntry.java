package org.epics.archiverappliance.utils.nio.gztar;

import java.io.File;

/*
 * Encapsulates both the tar entries in a tar file and also the inputs to the tar file
 */

public record TarEntry(
        String entryName, long headeroffset, long dataoffset, long size, long uncompressedsize, File srcContent) {

    public TarEntry(String entryName, File srcContent) {
        this(entryName, -1, -1, -1, -1, srcContent);
    }

    public TarEntry(String entryName, long headeroffset, long dataoffset, long size, long uncompressedsize) {
        this(entryName, headeroffset, dataoffset, size, uncompressedsize, null);
    }

    @Override
    public final String toString() {
        StringBuilder buf = new StringBuilder();
        if (srcContent == null) {
            buf.append("File: ")
                    .append(entryName)
                    .append(" starting at ")
                    .append(dataoffset)
                    .append(" and size ")
                    .append(size)
                    .append(" uncompressed ")
                    .append(uncompressedsize);
        } else {
            buf.append("File: ").append(entryName).append(" with source at ").append(srcContent.getAbsolutePath());
        }
        return buf.toString();
    }

    public boolean isInsideTar() {
        return dataoffset > 0;
    }

    public boolean isDeleted() {
        return entryName.startsWith(EAATar.LOGICAL_DELETE_PREFIX);
    }
}
;
