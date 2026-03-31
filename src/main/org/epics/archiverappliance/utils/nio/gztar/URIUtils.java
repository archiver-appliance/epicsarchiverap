package org.epics.archiverappliance.utils.nio.gztar;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;

public class URIUtils {
    private static final Logger logger = LogManager.getLogger(URIUtils.class.getName());

    public static final String GZTAR_SCHEME = "gztar";
    public static final String GZTAR_TERMINATOR = ".tar!";

    private static void checkScheme(URI uri) throws IllegalArgumentException {
        String scheme = uri.getScheme();
        if ((scheme == null) || !scheme.equalsIgnoreCase(GZTAR_SCHEME)) {
            throw new IllegalArgumentException("URI scheme is not '" + GZTAR_SCHEME + "'. It is " + scheme);
        }
    }

    public static Path getPathToTarFile(URI uri) {
        checkScheme(uri);
        String spec = uri.getRawSchemeSpecificPart();
        int sep = spec.lastIndexOf(GZTAR_TERMINATOR);
        if (sep != -1) {
            spec = spec.substring(0, sep);
        }
        spec = spec.replaceAll("^//", "");
        logger.debug("Path to tar file {}", spec);
        return Paths.get(spec);
    }

    public static String getPathWithinTarFile(URI uri) {
        // See if we can use nulls/empty strings/non empty strings to indicate path to file system, the tar file itself
        // and an entry in the tar file
        String spec = uri.getRawSchemeSpecificPart();
        int sep = spec.lastIndexOf(GZTAR_TERMINATOR);
        String finalPathComponent = spec.substring(sep + GZTAR_TERMINATOR.length());
        return finalPathComponent;
    }

    public static String generateURI(String pathToTarFile, String pathWithinTarFile) {
        StringBuilder buf = new StringBuilder();
        buf.append(GZTAR_SCHEME);
        buf.append("://");
        buf.append(pathToTarFile.replaceAll("\\.tar$", ""));
        buf.append(GZTAR_TERMINATOR);
        if (pathWithinTarFile != null && !pathWithinTarFile.isEmpty()) {
            buf.append(pathWithinTarFile);
        }
        String uri = buf.toString();
        logger.debug("Generated URI: {}", uri);
        return uri;
    }

    private static void appendPart(StringBuilder buf, String part, boolean isFirst) {
        if (part.contains(".pb") || part.contains(".parquet")) {
            buf.append(GZTAR_TERMINATOR);
            buf.append(part);
        } else {
            if (!isFirst) {
                buf.append("/");
            }
            buf.append(part);
        }
    }

    public static String combinePathElements(boolean createParent, String first, String... more) {
        if (!first.startsWith(GZTAR_SCHEME + "://")) {
            throw new IllegalArgumentException("The first part of the URI must start with the gztar scheme");
        }
        ArrayList<String> pathParts = new ArrayList<>();
        {
            String[] parts = first.split("/");
            for (String part : parts) {
                pathParts.add(part);
            }
        }
        for (String pathElem : more) {
            String[] parts = pathElem.split("/");
            for (String part : parts) {
                pathParts.add(part);
            }
        }
        int numParts = pathParts.size();
        String lastElement = pathParts.get(numParts - 1);
        if (lastElement.contains(".pb") || lastElement.contains(".parquet")) {

        } else {

        }

        boolean isFirst = true;
        StringBuilder buf = new StringBuilder();

        String combined = buf.toString();
        logger.debug("Combined paths: {}", combined);
        return combined;
    }
}
