package net.joshdevins.hadoop.utils.io;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import org.apache.commons.lang.Validate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;

/**
 * Copies all files from a local directory into a {@link SequenceFile}. In the final {@link SequenceFile}, the key is
 * a {@link Text} filename, the value is a {@link BytesWritable} of bytes from the file.
 * 
 * Any file that is not readable will be skipped. Sub-directories are not recursed into.
 * 
 * @see http://www.cloudera.com/blog/2009/02/the-small-files-problem
 * @see http://stuartsierra.com/2008/04/24/a-million-little-files
 * 
 * @author Josh Devins
 */
public final class FilesIntoSequenceFile {

    static class ExitException extends RuntimeException {

        private static final long serialVersionUID = 8960179968498981419L;
    }

    private static final String USAGE = "Usage: FilesIntoSequenceFile <input directory> <output SequenceFile>";

    public static void main(final String[] args) {

        Validate.isTrue(args.length == 2, USAGE);

        try {
            run(args[0], args[1]);
        } catch (ExitException ee) {
            // abnormal exit
            System.exit(1);
        }
    }

    /**
     * Run the darn thing. Refactored out from main method to enable unit testing.
     */
    public static void run(final String input, final String output) {

        Validate.notEmpty(input, USAGE);
        Validate.notEmpty(output, USAGE);

        Configuration conf = new Configuration();

        // confirm redable input dir
        File inputDir = new File(input);
        if (!inputDir.isDirectory() || !inputDir.canRead()) {
            exitWithError("Input is not a readable directory: " + input);
        }

        // get input files
        File[] inputFiles = inputDir.listFiles();
        if (inputFiles == null || inputFiles.length == 0) {
            exitWithError("No input files to process in directory: " + input);
        }

        // setup output file, no compression
        SequenceFile.Writer writer = null;
        try {
            FileSystem outputFS = FileSystem.get(URI.create(output), conf);
            writer = SequenceFile.createWriter(outputFS, conf, new Path(output), Text.class, BytesWritable.class,
                    SequenceFile.CompressionType.NONE);

        } catch (IOException ioe) {
            exitWithStackTraceAndError("Error creating output SequenceFile: " + output, ioe);
        }

        // write every file in the directory to the sequence file, ignoring any sub-directories or unreadable files
        for (File inputFile : inputFiles) {

            if (inputFile.isDirectory()) {
                System.out.println("Skipping subdirectory: " + inputFile.getName());
                continue;
            }

            if (!inputFile.canRead()) {
                System.err.println("Skipping unreadable file: " + inputFile.getName());
                continue;
            }

            appendFileToWriter(inputFile, writer);
        }

        IOUtils.closeStream(writer);
    }

    /**
     * Appends the file to the writer. IOExceptions will all be caught internally and any errors printed to stderr. This
     * is to aid in simply skipping errors and moving along.
     */
    private static boolean appendFileToWriter(final File file, final Writer writer) {

        String key = file.getName();

        byte[] bytes = null;
        try {
            bytes = getBytesFromFile(file);

        } catch (IOException ioe) {
            printStackTraceAndError(ioe, "Failed to read file: " + key);
            return false;
        }

        try {
            writer.append(new Text(key), new BytesWritable(bytes));

        } catch (IOException ioe) {
            printStackTraceAndError(ioe, "Failed to append to output SequenceFile");
            return false;
        }

        return true;
    }

    private static void exitWithError(final String message) {
        System.err.println(message);
        throw new ExitException();
    }

    private static void exitWithStackTraceAndError(final String message, final Exception e) {
        e.printStackTrace();
        exitWithError(message);
    }

    /**
     * Loads a file into a byte[].
     * 
     * @see http://www.exampledepot.com/egs/java.io/File2ByteArray.html
     */
    private static byte[] getBytesFromFile(final File file) throws IOException {

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

    private static void printStackTraceAndError(final Exception e, final String message) {
        e.printStackTrace();
        System.err.println(message);
    }
}
