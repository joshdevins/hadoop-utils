package net.joshdevins.hadoop.utils.io;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import org.apache.commons.lang.Validate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.BloomMapFile;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;

/**
 * Copies all files from a local directory into a {@link BloomMapFile}. In the final {@link BloomMapFile}, the key is
 * a {@link Text} filename, the value is a {@link BytesWritable} of bytes from the file.
 * 
 * <p>
 * Any file that is not readable will be skipped. Sub-directories are not recursed into.
 * </p>
 * 
 * <p>
 * TODO: Refactor to share more code with {@link FilesIntoSequenceFile} since they are basically the same.
 * </p>
 * 
 * @see FilesIntoSequenceFile
 * 
 * @author Josh Devins
 */
public final class FilesIntoBloomMapFile {

    private static final String USAGE = "Usage: FilesIntoBloomMapFile <input directory> <output BloomMapFile>";

    public static void main(final String[] args) {

        Validate.notNull(args, USAGE);
        Validate.isTrue(args.length == 2, USAGE);

        try {
            run(args[0], args[1]);
        } catch (ExitException ee) {
            // abnormal exit
            System.exit(1);
        }
    }

    public static void run(final String input, final String output) {

        Validate.notEmpty(input, USAGE);
        Validate.notEmpty(output, USAGE);

        Configuration conf = new Configuration();

        File[] inputFiles = FilesIntoSequenceFile.getInputFiles(input);

        // setup output file, no compression
        BloomMapFile.Writer writer = null;
        try {
            FileSystem outputFS = FileSystem.get(URI.create(output), conf);
            writer = new BloomMapFile.Writer(conf, outputFS, output, Text.class, BytesWritable.class,
                    CompressionType.NONE);

        } catch (IOException ioe) {
            MainUtils.exitWithStackTraceAndError("Error creating output BloomMapFile: " + output, ioe);
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
    static boolean appendFileToWriter(final File file, final BloomMapFile.Writer writer) {

        String key = file.getName();

        byte[] bytes = null;
        try {
            bytes = FileUtils.getBytesFromFile(file);

        } catch (IOException ioe) {
            MainUtils.printStackTraceAndError(ioe, "Failed to read file: " + key);
            return false;
        }

        try {
            writer.append(new Text(key), new BytesWritable(bytes));

        } catch (IOException ioe) {
            MainUtils.printStackTraceAndError(ioe, "Failed to append to output BloomMapFile");
            return false;
        }

        return true;
    }
}
