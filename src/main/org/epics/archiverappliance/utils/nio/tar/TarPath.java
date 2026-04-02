package org.epics.archiverappliance.utils.nio.tar;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileSystem;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.WatchEvent.Kind;
import java.nio.file.WatchEvent.Modifier;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;

/*
 * A GZPath consists of the tar file ( represented by the fileSystem ) and the entry in the tar file.
 * A null tarEntry acts as a Path for the tar file itself.
 * We don't really maintain directories in the tar file; the GZTar is a two level file system
 */
public class TarPath implements Path {
    private static final Logger logger = LogManager.getLogger(TarPath.class.getName());

    final TarFileSystem fileSystem;
    final TarEntry tarEntry;

    public TarPath(TarFileSystem fileSystem, TarEntry tarEntry) {
        this.fileSystem = fileSystem;
        this.tarEntry = tarEntry;
    }

    @Override
    public FileSystem getFileSystem() {
        return this.fileSystem;
    }

    @Override
    public boolean isAbsolute() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'isAbsolute'");
    }

    @Override
    public Path getRoot() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getRoot'");
    }

    @Override
    public Path getFileName() {
        return Paths.get(this.tarEntry.entryName());
    }

    /*
     * We treat TAR entries similar to hashes in HTML.
     * The parent of all GZTar paths is the parent folder of the tar file.
     */
    @Override
    public Path getParent() {
        if (this.tarEntry != null) {
            logger.debug(
                    "Getting parent of GZPath {} + {}", this.fileSystem.tarPath.toString(), this.tarEntry.entryName());
            return new TarPath(this.fileSystem, null);
        }

        logger.debug("Getting parent of tar file itself {}", this.fileSystem.tarPath.toString());
        return this.fileSystem.tarPath.getParent();
    }

    @Override
    public int getNameCount() {
        if (this.tarEntry != null) {
            return this.fileSystem.tarPath.getNameCount() + 1;
        } else {
            return this.fileSystem.tarPath.getNameCount();
        }
    }

    @Override
    public Path getName(int index) {
        if (index < this.fileSystem.tarPath.getNameCount()) {
            return this.fileSystem.tarPath.getName(index);
        }

        if (this.tarEntry != null) {
            return Paths.get(this.tarEntry.entryName());
        }
        throw new IllegalArgumentException("Index " + index + " is out of bounds");
    }

    @Override
    public Path subpath(int beginIndex, int endIndex) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'subpath'");
    }

    @Override
    public boolean startsWith(Path other) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'startsWith'");
    }

    @Override
    public boolean endsWith(Path other) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'endsWith'");
    }

    @Override
    public Path normalize() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'normalize'");
    }

    @Override
    public Path resolve(Path other) {
        if (this.tarEntry != null) {
            logger.debug(
                    "Resolving path {} against {}",
                    other.toString(),
                    this.toUri().toString());
            throw new UnsupportedOperationException("We do not support resolving paths against entries yet");
        }

        TarEntry entry = this.fileSystem.lookupTarEntry(other);
        if (entry == null) {
            logger.debug(
                    "No entry found for {}, returning empty GZPath",
                    other.toUri().toString());
            return new TarPath(this.fileSystem, null);
        } else {
            logger.debug(
                    "Found entry for {}, returning GZPath for tar entry {}",
                    other.toUri().toString(),
                    entry);
            return new TarPath(this.fileSystem, entry);
        }
    }

    @Override
    public Path relativize(Path other) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'relativize'");
    }

    @Override
    public URI toUri() {
        try {
            return new URI(URIUtils.generateURI(
                    this.fileSystem.tarPath.toString(), this.tarEntry != null ? this.tarEntry.entryName() : ""));
        } catch (URISyntaxException ex) {
            ex.printStackTrace();
            return null;
        }
    }

    @Override
    public Path toAbsolutePath() {
        return this;
    }

    @Override
    public Path toRealPath(LinkOption... options) throws IOException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'toRealPath'");
    }

    @Override
    public WatchKey register(WatchService watcher, Kind<?>[] events, Modifier... modifiers) throws IOException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'register'");
    }

    @Override
    public int compareTo(Path other) {
        if (other instanceof TarPath) {
            TarPath otp = (TarPath) other;
            if (this.tarEntry != null && otp.tarEntry != null) {
                return this.tarEntry.entryName().compareTo(otp.tarEntry.entryName());
            }
        }
        return this.toUri().compareTo(other.toUri());
    }

    public TarEntry getTarEntry() {
        return this.tarEntry;
    }

    @Override
    public String toString() {
        return this.toUri().toString();
    }

    private static BasicFileAttributes readAttributes(TarFileSystem fileSystem, TarEntry tarEntry) {
        return new BasicFileAttributes() {

            @Override
            public FileTime lastModifiedTime() {
                return FileTime.fromMillis(System.currentTimeMillis());
            }

            @Override
            public FileTime lastAccessTime() {
                return FileTime.fromMillis(System.currentTimeMillis());
            }

            @Override
            public FileTime creationTime() {
                return FileTime.fromMillis(System.currentTimeMillis());
            }

            @Override
            public boolean isRegularFile() {
                return true;
            }

            @Override
            public boolean isDirectory() {
                if (tarEntry != null
                        && tarEntry.entryName() != null
                        && !tarEntry.entryName().endsWith("/")) {
                    return false;
                }
                return true;
            }

            @Override
            public boolean isSymbolicLink() {
                return false;
            }

            @Override
            public boolean isOther() {
                return false;
            }

            @Override
            public long size() {
                if (tarEntry == null) {
                    return -1;
                }

                TarEntry latest = fileSystem.lookupTarEntry(tarEntry.entryName(), tarEntry);
                logger.debug("Getting size for tar entry {}: {}", latest.entryName(), latest.size());
                return latest.size();
            }

            @Override
            public Object fileKey() {
                throw new UnsupportedOperationException("Unimplemented method 'fileKey'");
            }
        };
    }

    @SuppressWarnings("unchecked")
    public <A extends BasicFileAttributes> A getBasicFileAttributes(Class<A> type, LinkOption... options)
            throws IOException {
        if (tarEntry == null) {
            throw new IOException(
                    "We should not be using the GZTar implementaiton to get the attributes of the default file system");
        } else {
            try {
                if (type == BasicFileAttributes.class) {
                    return (A) readAttributes(this.fileSystem, this.tarEntry);
                }

                throw new UnsupportedOperationException("Attributes of type " + type.getName() + " not supported");

            } catch (Exception ex) {
                throw new IOException(ex);
            }
        }
    }
}
