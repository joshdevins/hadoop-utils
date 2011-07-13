package net.joshdevins.hadoop.utils.io;

import java.io.IOException;
import java.net.URI;
import java.util.LinkedList;
import java.util.List;

import net.joshdevins.hadoop.utils.Pair;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;

/**
 * General utilities for HDFS and Hadoop files.
 * 
 * @author Josh Devins
 */
public final class HdfsUtils {

    private HdfsUtils() {
        throw new UnsupportedOperationException();
    }

    /**
     * Reads a {@link SequenceFile} and returns a {@link List} of {@link Pair}s containing the key and value. Note that
     * this is highly inefficient since it loads everything into memory and requires new key and value class instances
     * for each row of the {@link SequenceFile}. Generally this should be used only for small-ish {@link SequenceFile}s.
     */
    @SuppressWarnings("unchecked")
    public static <K extends Writable, V extends Writable> List<Pair<K, V>> readSequenceFile(final FileSystem fs,
            final Path path, final Configuration conf) throws IOException {

        SequenceFile.Reader reader = null;
        LinkedList<Pair<K, V>> list = new LinkedList<Pair<K, V>>();

        try {
            reader = new SequenceFile.Reader(fs, path, conf);
            K key = (K) reader.getKeyClass().newInstance();
            V value = (V) reader.getValueClass().newInstance();

            while (reader.next(key, value)) {
                list.add(new Pair<K, V>(key, value));

                // new instances since they are passed back by reference in Pair and the List
                key = (K) reader.getKeyClass().newInstance();
                value = (V) reader.getValueClass().newInstance();
            }

        } catch (IllegalAccessException iae) {
            throw new IllegalStateException("Error instantiating key or value class", iae);

        } catch (InstantiationException ie) {
            throw new IllegalStateException("Error instantiating key or value class", ie);

        } finally {
            IOUtils.closeStream(reader);
        }

        return list;
    }

    /**
     * @see #readSequenceFile(FileSystem, Path, Configuration)
     */
    public static <K extends Writable, V extends Writable> List<Pair<K, V>> readSequenceFile(final String uri)
            throws IOException {

        return readSequenceFile(uri, new Configuration());
    }

    /**
     * @see #readSequenceFile(FileSystem, Path, Configuration)
     */
    public static <K extends Writable, V extends Writable> List<Pair<K, V>> readSequenceFile(final String uri,
            final Configuration conf) throws IOException {

        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        Path path = new Path(uri);

        return readSequenceFile(fs, path, conf);
    }
}
