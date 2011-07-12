package net.joshdevins.hadoop.utils.io.converter;

import java.io.IOException;
import java.net.URI;

import net.joshdevins.hadoop.utils.MainUtils;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.Tool;

/**
 * Converts a {@link SequenceFile} into a {@link MapFile}. This will move the input {@link SequenceFile} to the location
 * specified by the output {@link MapFile}. The filesystem to operate on is specified by the input and the standard
 * Hadoop configuration mechanisms.
 * 
 * Example:
 * <ul>
 * <li>input: sequencefiles/part-r-00000</li>
 * <li>output: mapfiles/00000</li>
 * <li>final result: mapfiles/00000/data, mapfiles/00000/index</li>
 * </ul>
 * 
 * @see "Hadoop: The Definitive Guide", 2nd Ed., Tom White, p. 127 -- Converting a SequenceFile to a MapFile
 * 
 * @author Josh Devins
 */
public final class SequenceFileToMapFile extends Configured implements Tool {

    @Override
    @SuppressWarnings("unchecked")
    public int run(final String[] args) throws Exception {

        MainUtils.validateStandardInputOutputDriver(SequenceFileToMapFile.class, args);

        String input = args[0];
        String output = args[1];

        // setup input and output files
        FileSystem fs = null;
        try {
            fs = FileSystem.get(URI.create(input), getConf());
        } catch (IOException ioe) {
            MainUtils.exitWithStackTraceAndError("Failed to get FileSystem of input file: " + input, ioe);
        }

        // setup file paths
        Path sequenceFile = new Path(input);
        Path mapFile = new Path(output);
        Path mapData = new Path(mapFile, MapFile.DATA_FILE_NAME);
        Path mapIndex = new Path(mapFile, MapFile.INDEX_FILE_NAME);

        // check to see if destination files already exists
        try {
            if (fs.exists(mapFile) || fs.exists(mapData) || fs.exists(mapIndex)) {
                MainUtils.exitWithError("MapFile already exists: " + output);
            }
        } catch (IOException ioe) {
            MainUtils.exitWithStackTraceAndError("Failed to check filesystem for pre-existing MapFile: " + output, ioe);
        }

        // get key and value types from SequenceFile
        SequenceFile.Reader reader = null;
        Class<? extends Writable> keyClass = null;
        Class<? extends Writable> valueClass = null;
        try {
            reader = new SequenceFile.Reader(fs, sequenceFile, getConf());
            keyClass = (Class<? extends Writable>) reader.getKeyClass();
            valueClass = (Class<? extends Writable>) reader.getValueClass();

        } catch (IOException ioe) {
            MainUtils.exitWithStackTraceAndError(
                    "Failed to open SequenceFile to determine key/value classes: " + input, ioe);
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException ioe) {
                    // ignore
                }
            }
        }

        // move the SequenceFile to the new map file location, rename it to "data" within the output location
        try {
            fs.rename(sequenceFile, mapData);
        } catch (IOException ioe) {
            MainUtils.exitWithStackTraceAndError(
                    "Failed to move SequenceFile to data file in MapFile directory: input=" + input + ", output="
                            + output, ioe);
        }

        // create the MapFile index file
        try {
            MapFile.fix(fs, mapFile, keyClass, valueClass, false, getConf());
        } catch (Exception e) {
            MainUtils.exitWithStackTraceAndError("Failed to create MapFile index: " + output, e);
        }

        return 0;
    }

    public static void main(final String[] args) throws Exception {
        MainUtils.toolRunner(new SequenceFileToMapFile(), args);
    }
}
