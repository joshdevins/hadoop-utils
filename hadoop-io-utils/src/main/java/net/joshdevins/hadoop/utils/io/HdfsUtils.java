package net.joshdevins.hadoop.utils.io;

import java.io.IOException;
import java.net.URI;
import java.util.LinkedList;
import java.util.List;

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

    public static <K extends Writable, V extends Writable> List<Pair<K, V>> readSequenceFile(final String uri)
            throws IOException {

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        Path path = new Path(uri);

        return readSequenceFile(fs, path, conf);
    }
}
