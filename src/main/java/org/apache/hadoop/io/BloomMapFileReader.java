package org.apache.hadoop.io;

import java.io.DataInputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.bloom.DynamicBloomFilter;
import org.apache.hadoop.util.bloom.Filter;
import org.apache.hadoop.util.bloom.Key;

/**
 * A re-implementation of the standard {@link BloomMapFile.Reader} to overcome a known bug in
 * {@link #probablyHasKey(WritableComparable)}. This is a straight copy of {@link BloomMapFile.Reader} with the fix
 * applied.
 * 
 * @see <a href="https://issues.apache.org/jira/browse/HADOOP-6546">HADOOP-6546</a>
 * 
 * @author Josh Devins
 */
public class BloomMapFileReader extends MapFile.Reader {

    private DynamicBloomFilter bloomFilter;
    private final DataOutputBuffer buf = new DataOutputBuffer();
    private final Key bloomKey = new Key();

    public BloomMapFileReader(final FileSystem fs, final String dirName, final Configuration conf) throws IOException {
        super(fs, dirName, conf);
        initBloomFilter(fs, dirName, conf);
    }

    public BloomMapFileReader(final FileSystem fs, final String dirName, final WritableComparator comparator,
            final Configuration conf) throws IOException {
        super(fs, dirName, comparator, conf);
        initBloomFilter(fs, dirName, conf);
    }

    public BloomMapFileReader(final FileSystem fs, final String dirName, final WritableComparator comparator,
            final Configuration conf, final boolean open) throws IOException {
        super(fs, dirName, comparator, conf, open);
        initBloomFilter(fs, dirName, conf);
    }

    /**
     * Fast version of the {@link MapFile.Reader#get(WritableComparable, Writable)} method. First
     * it checks the Bloom filter for the existence of the key, and only if
     * present it performs the real get operation. This yields significant
     * performance improvements for get operations on sparsely populated files.
     */
    @SuppressWarnings("rawtypes")
    @Override
    public synchronized Writable get(final WritableComparable key, final Writable val) throws IOException {

        if (!probablyHasKey(key)) {
            return null;
        }

        return super.get(key, val);
    }

    /**
     * Retrieve the Bloom filter used by this instance of the Reader.
     * 
     * @return a Bloom filter (see {@link Filter})
     */
    public Filter getBloomFilter() {
        return bloomFilter;
    }

    /**
     * Checks if this MapFile has the indicated key. The membership test is
     * performed using a Bloom filter, so the result has always non-zero
     * probability of false positives.
     * 
     * @param key
     *        key to check
     * @return false iff key doesn't exist, true if key probably exists.
     * @throws IOException
     */
    @SuppressWarnings("rawtypes")
    public boolean probablyHasKey(final WritableComparable key) throws IOException {

        if (bloomFilter == null) {
            return true;
        }

        buf.reset();
        key.write(buf);

        // patch from HADOOP-6546
        bloomKey.set(BloomMapFileWriter.byteArrayForBloomKey(buf), 1.0);
        return bloomFilter.membershipTest(bloomKey);
    }

    private void initBloomFilter(final FileSystem fs, final String dirName, final Configuration conf)
            throws IOException {

        DataInputStream in = fs.open(new Path(dirName, BloomMapFile.BLOOM_FILE_NAME));
        bloomFilter = new DynamicBloomFilter();
        bloomFilter.readFields(in);
        in.close();
    }
}
