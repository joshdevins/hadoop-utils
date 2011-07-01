package net.joshdevins.hadoop.utils.io;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

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
        return IOUtils.getBytesFromInputStream(new FileInputStream(file));
    }

    /**
     * Sorts an array of files by filename, ascending.
     */
    public static File[] sortFiles(final File[] files) {

        File[] rtn = Arrays.copyOf(files, files.length);
        Arrays.sort(rtn, new Comparator<File>() {

            @Override
            public int compare(final File file1, final File file2) {
                return file1.getName().compareTo(file2.getName());
            }
        });

        return rtn;
    }
}
