package org.epics.archiverappliance.utils.nio.tar;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.etl.ETLOptimizable;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.nio.file.WatchService;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.nio.file.spi.FileSystemProvider;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/*
 * An NIO2 file system that encapsulates a tar file containing gzipped pb files.
 * This is intended to be user as a extra-long-term-store where performance of retrieval is not necessarily a requirement.
 * There is limited support for updating a file; updating an existing entry relies on logical deletes and is extrememly inefficient.
 * One can optimize a bloated gztar file by copying into a new file and discarding all the logically deleted blocks ( similar to MySQL's optimize )
 * Only the final component of the path is used to identify the tar entry.
 * Given a path of the form gztar:///arch/XLTS/mshankar/arch/gztartest:2016.pb, we map it to the tar file /arch/XLTS/mshankar/arch/gztartest.tar
 * and the entry gztartest:2016.pb within the tar file.
 * We do not maintain nested paths inside a GZTar file system; the tar catalog is treated as a single directory containing all the entries in the tar file.
 * Also, for now, we do not support links, devices and other tar features.
 * Because updates and deletes are inefficient logical operations, a GZTar data store is expected to be the very last data store in a PVTypeInfo.
 * ETL into a GZTar data store is expected to be infrequent ( maybe once a year )
 * ETL out of a GZTar data store is not expected to happen.
 */
public class TarFileSystem extends FileSystem implements ETLOptimizable {
    private static final Logger logger = LogManager.getLogger(TarFileSystem.class.getName());

    private TarFileSystemProvider provider;
    final Path tarPath;
    final Map<String, ?> env;
    final EAATar tarFile;

    private Map<String, TarEntry> tarFileCatalog;

    TarFileSystem(TarFileSystemProvider provider, Path tarPath, Map<String, ?> env) throws IOException {
        logger.debug("Creating GZTarFileSystem for {}", tarPath.toString());
        this.provider = provider;
        this.tarPath = tarPath;
        this.env = env;
        this.tarFile = new EAATar(this.tarPath.toString());
        this.tarFileCatalog = this.tarFile.loadCatalog();
        logger.debug("Loaded {} catalog with {} entries", tarPath.toString(), this.tarFileCatalog.size());
    }

    @Override
    public void close() throws IOException {
        logger.debug("Closing file system for {}", this.tarPath);
        this.provider.closeFileSystem(this.tarFile.getTarFileName(), this);
    }

    @Override
    public Iterable<FileStore> getFileStores() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Path getPath(String first, String... more) {
        Path combined = Paths.get(first, more);
        try {
            logger.debug("First: {} Rest: {}", first, String.join(",", more));
            TarEntry entry = null;
            String entryName = combined.toString();
            if (entryName == null || entryName.isEmpty() || entryName.equals("/")) {
                logger.debug("Asking for the file system root");
                return new TarPath(this, new TarEntry("/", null));
            }

            entry = this.tarFileCatalog.get(entryName);

            if (entry == null) {
                logger.debug("Empty GZPath for {}", entryName);
                return new TarPath(this, new TarEntry(entryName, null));

            } else {
                logger.debug("GZPath for tar entry {}", entry);
                return new TarPath(this, entry);
            }
        } catch (Exception ex) {
            logger.error("Exception parsing URI " + combined.toString(), ex);
            return null;
        }
    }

    @Override
    public PathMatcher getPathMatcher(String syntaxAndPattern) {
        PathMatcher m = FileSystems.getDefault().getPathMatcher(syntaxAndPattern);
        if (m == null) {
            throw new UnsupportedOperationException("PathMatcher '" + syntaxAndPattern + "' not recognized");
        }

        return new PathMatcher() {
            @Override
            public boolean matches(Path path) {
                logger.debug("Checking to see if we can match path {} against {}", path.toString(), syntaxAndPattern);
                return m.matches(path);
            }
        };
    }

    @Override
    public Iterable<Path> getRootDirectories() {
        logger.debug("Getting root directories for {}", this.tarFile.toString());
        return List.of(this.tarPath);
    }

    @Override
    public String getSeparator() {
        logger.debug("Getting separator");
        return "/";
    }

    @Override
    public UserPrincipalLookupService getUserPrincipalLookupService() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isOpen() {
        logger.debug("Checking to see if filesystem is open");
        return true;
    }

    @Override
    public boolean isReadOnly() {
        logger.debug("Checking to see if filesystem is readonly");
        return false;
    }

    @Override
    public WatchService newWatchService() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public FileSystemProvider provider() {
        return this.provider;
    }

    @Override
    public Set<String> supportedFileAttributeViews() {
        throw new UnsupportedOperationException();
    }

    public Path getTarPath() {
        return tarPath;
    }

    public EAATar getTarFile() {
        return tarFile;
    }

    public Map<String, TarEntry> getTarFileCatalog() {
        return Collections.unmodifiableMap(tarFileCatalog);
    }

    public TarEntry lookupTarEntry(String entryName) {
        if (entryName != null && entryName.startsWith("/")) {
            entryName = entryName.substring(1);
        }
        return this.tarFileCatalog.get(entryName);
    }

    public TarEntry lookupTarEntry(String entryName, TarEntry defaultEntry) {
        TarEntry entry = lookupTarEntry(entryName);
        return entry != null ? entry : defaultEntry;
    }

    public TarEntry lookupTarEntry(Path path) {
        return lookupTarEntry(URIUtils.getPathWithinTarFile(path.toUri()));
    }

    public void reloadCatalog() throws IOException {
        this.tarFileCatalog = this.tarFile.loadCatalog();
    }

    @Override
    public boolean equals(Object obj) {
        TarFileSystem other = (TarFileSystem) obj;
        return this.tarPath.equals(other.tarPath);
    }

    public boolean needsOptimization() throws IOException {
        return this.tarFile.hasDeletedEntries();
    }

    @Override
    public boolean optimize() throws IOException {
        if (!needsOptimization()) {
            logger.info("No optimization needed for {}", this.tarPath);
            return false;
        }
        this.tarFileCatalog = this.tarFile.loadCatalog();
        try {
            this.tarFile.optimize();
            this.tarFileCatalog = this.tarFile.loadCatalog();
            logger.info("Optimized GZTar file {} successfully", this.tarPath);
            return true;
        } catch (IOException e) {
            logger.error("Optimization of GZTar file " + this.tarPath + " failed", e);
        }
        return false;
    }
}
