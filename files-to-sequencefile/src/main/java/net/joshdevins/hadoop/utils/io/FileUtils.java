package net.joshdevins.hadoop.utils.io;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

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

    /**
     * Loads a file into a byte[].
     * 
     * @see http://www.exampledepot.com/egs/java.io/File2ByteArray.html
     */
    public static byte[] getBytesFromFile(final File file) throws IOException {

        InputStream is = new FileInputStream(file);

        // Get the size of the file
        long length = file.length();

        // You cannot create an array using a long type.
        // It needs to be an int type.
        // Before converting to an int type, check
        // to ensure that file is not larger than Integer.MAX_VALUE.
        if (length > Integer.MAX_VALUE) {
            // File is too large
            is.close();
            throw new IOException("File is too large to fit in an array of max size Integer.MAX_VALUE: " + length);
        }

        // Create the byte array to hold the data
        byte[] bytes = new byte[(int) length];

        // Read in the bytes
        int offset = 0;
        int numRead = 0;
        try {
            while (offset < bytes.length && (numRead = is.read(bytes, offset, bytes.length - offset)) >= 0) {
                offset += numRead;
            }

        } catch (IOException ioe) {
            throw ioe;

        } finally {
            // always close input stream no matter what
            is.close();
        }

        // Ensure all the bytes have been read in
        if (offset < bytes.length) {
            throw new IOException("Could not read entire file: " + file.getName());
        }

        // Close the input stream and return bytes
        return bytes;
    }
}
