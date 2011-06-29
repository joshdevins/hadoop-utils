package net.joshdevins.hadoop.utils.io;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.lang.Validate;

/**
 * Simple file based utilities for regular filesystems.
 * 
 * @author Josh Devins
 */
public final class FileUtils {

    private FileUtils() {
        throw new UnsupportedOperationException();
    }

    /**
     * Creates a directory and deletes it if it already exists.
     */
    public static boolean createDirectoryDestructive(final String directory) {

        deleteDirectory(directory);
        return new File(directory).mkdirs();
    }

    /**
     * Recursively deletes a directory and all its' contents.
     * 
     * @return true if the deletion was successful, false otherwise
     * 
     * @see http://www.java2s.com/Tutorial/Java/0180__File/Removeadirectoryandallofitscontents.htm
     */
    public static boolean deleteDirectory(final File directory) {

        if (directory == null) {
            return false;
        }

        if (!directory.exists()) {
            return true;
        }

        if (!directory.isDirectory()) {
            return false;
        }

        String[] list = directory.list();

        if (list != null) {
            for (String element : list) {

                File entry = new File(directory, element);

                if (entry.isDirectory()) {
                    if (!deleteDirectory(entry)) {
                        return false;
                    }

                } else {
                    if (!entry.delete()) {
                        return false;
                    }
                }
            }
        }

        return directory.delete();
    }

    public static boolean deleteDirectory(final String directory) {
        return deleteDirectory(new File(directory));
    }

    public static byte[] getBytesFromFile(final File file) throws IOException {

        Validate.notNull(file);
        return getBytesFromInputStream(new FileInputStream(file));
    }

    /**
     * Reads bytes from the {@link InputStream}. The input {@link InputStream} will be closed regardless of exceptions.
     */
    public static byte[] getBytesFromInputStream(final InputStream is) throws IOException {

        Validate.notNull(is);
        BufferedInputStream bis = new BufferedInputStream(is);

        try {
            byte[] buffer = new byte[bis.available()];
            bis.read(buffer);

            return buffer;

        } finally {
            try {
                bis.close();
            } finally {
                is.close();
            }
        }
    }

    public static byte[] getBytesFromResource(final String resource) throws IOException {

        Validate.notEmpty(resource);
        return getBytesFromInputStream(FileUtils.class.getResourceAsStream(resource));
    }
}
