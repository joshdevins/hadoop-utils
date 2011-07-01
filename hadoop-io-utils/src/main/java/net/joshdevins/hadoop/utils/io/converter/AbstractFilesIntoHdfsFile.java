package net.joshdevins.hadoop.utils.io.converter;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.URI;

import net.joshdevins.hadoop.utils.io.FileUtils;
import net.joshdevins.hadoop.utils.io.MainUtils;

import org.apache.commons.lang.Validate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;

/**
 * Copies all files from a local directory into some sort of HDFS file (dependent on implementation). In the final
 * output HDFS file, the key is a {@link Text} filename, the value is a {@link BytesWritable} of bytes from the file.
 * 
 * <p>
 * Any file that is not readable will be skipped. Sub-directories are not recursed into.
 * </p>
 * 
 * @see http://www.cloudera.com/blog/2009/02/the-small-files-problem
 * @see http://stuartsierra.com/2008/04/24/a-million-little-files
 * 
 * @author Josh Devins
 */
public abstract class AbstractFilesIntoHdfsFile<W extends Closeable> {

    private final String input;

    private final String output;

    private final Configuration config;

    /**
     * Default constructor.
     */
    public AbstractFilesIntoHdfsFile(final String input, final String output, final String usage) {

        Validate.notEmpty(input, usage);
        Validate.notEmpty(output, usage);

        this.input = input;
        this.output = output;
        this.config = new Configuration();
    }

    public Configuration getConfig() {
        return config;
    }

    public String getInput() {
        return input;
    }

    public String getOutput() {
        return output;
    }

    public void run() {

        File[] inputFiles = getInputFiles(input);

        // setup output file, no compression
        W writer = null;
        try {
            FileSystem outputFS = FileSystem.get(URI.create(output), config);
            writer = createWriter(outputFS);

        } catch (IOException ioe) {
            MainUtils.exitWithStackTraceAndError("Error creating output file: " + output, ioe);
        }

        // write every file in the directory to the sequence file, ignoring any sub-directories or unreadable files
        for (File inputFile : inputFiles) {

            if (inputFile.isDirectory()) {
                System.err.println("Skipping subdirectory: " + inputFile.getName());
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

    protected abstract void appendFilenameAndBytesToWriter(String key, byte[] bytes, W writer) throws IOException;

    protected abstract W createWriter(FileSystem outputFS) throws IOException;

    private boolean appendFileToWriter(final File file, final W writer) {

        String key = file.getName();

        byte[] bytes = null;
        try {
            bytes = FileUtils.getBytesFromFile(file);

        } catch (IOException ioe) {
            MainUtils.printStackTraceAndError(ioe, "Failed to read file: " + key);
            return false;
        }

        try {
            appendFilenameAndBytesToWriter(key, bytes, writer);

        } catch (IOException ioe) {
            MainUtils.printStackTraceAndError(ioe, "Failed to append to output SequenceFile");
            return false;
        }

        return true;
    }

    public static File[] getInputFiles(final String input) {

        // confirm redable input dir
        File inputDir = new File(input);
        if (!inputDir.isDirectory() || !inputDir.canRead()) {
            MainUtils.exitWithError("Input is not a readable directory: " + input);
        }

        // get input files
        File[] inputFiles = inputDir.listFiles();
        if (inputFiles == null || inputFiles.length == 0) {
            MainUtils.exitWithError("No input files to process in directory: " + input);
        }

        return FileUtils.sortFiles(inputFiles);
    }
}
