package org.apache.hadoop.io;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile.Writer;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.bloom.DynamicBloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;

/**
 * A re-implementation of the standard {@link BloomMapFile.Writer} to overcome a known bug in
 * {@link #append(WritableComparable, Writable)}. This is a straight copy of {@link BloomMapFile.Writer} with the fix
 * applied.
 * 
 * @see <a href="https://issues.apache.org/jira/browse/HADOOP-6546">HADOOP-6546</a>
 * 
 * @author Josh Devins
 */
public class BloomMapFileWriter extends Writer {

    private DynamicBloomFilter bloomFilter;
    private int numKeys;
    private int vectorSize;
    private final Key bloomKey = new Key();
    private final DataOutputBuffer buf = new DataOutputBuffer();
    private final FileSystem fs;
    private final Path dir;

    @SuppressWarnings("rawtypes")
    public BloomMapFileWriter(final Configuration conf, final FileSystem fs, final String dirName,
            final Class<? extends WritableComparable> keyClass, final Class valClass) throws IOException {
        super(conf, fs, dirName, keyClass, valClass);
        this.fs = fs;
        this.dir = new Path(dirName);
        initBloomFilter(conf);
    }

    @SuppressWarnings("rawtypes")
    public BloomMapFileWriter(final Configuration conf, final FileSystem fs, final String dirName,
            final Class<? extends WritableComparable> keyClass, final Class valClass, final CompressionType compress)
            throws IOException {
        super(conf, fs, dirName, keyClass, valClass, compress);
        this.fs = fs;
        this.dir = new Path(dirName);
        initBloomFilter(conf);
    }

    @SuppressWarnings("rawtypes")
    public BloomMapFileWriter(final Configuration conf, final FileSystem fs, final String dirName,
            final Class<? extends WritableComparable> keyClass, final Class valClass, final CompressionType compress,
            final Progressable progress) throws IOException {
        super(conf, fs, dirName, keyClass, valClass, compress, progress);
        this.fs = fs;
        this.dir = new Path(dirName);
        initBloomFilter(conf);
    }

    @SuppressWarnings("rawtypes")
    public BloomMapFileWriter(final Configuration conf, final FileSystem fs, final String dirName,
            final Class<? extends WritableComparable> keyClass, final Class<? extends Writable> valClass,
            final CompressionType compress, final CompressionCodec codec, final Progressable progress)
            throws IOException {
        super(conf, fs, dirName, keyClass, valClass, compress, codec, progress);
        this.fs = fs;
        this.dir = new Path(dirName);
        initBloomFilter(conf);
    }

    @SuppressWarnings("rawtypes")
    public BloomMapFileWriter(final Configuration conf, final FileSystem fs, final String dirName,
            final WritableComparator comparator, final Class valClass) throws IOException {
        super(conf, fs, dirName, comparator, valClass);
        this.fs = fs;
        this.dir = new Path(dirName);
        initBloomFilter(conf);
    }

    @SuppressWarnings("rawtypes")
    public BloomMapFileWriter(final Configuration conf, final FileSystem fs, final String dirName,
            final WritableComparator comparator, final Class valClass, final CompressionType compress)
            throws IOException {
        super(conf, fs, dirName, comparator, valClass, compress);
        this.fs = fs;
        this.dir = new Path(dirName);
        initBloomFilter(conf);
    }

    @SuppressWarnings("rawtypes")
    public BloomMapFileWriter(final Configuration conf, final FileSystem fs, final String dirName,
            final WritableComparator comparator, final Class valClass, final CompressionType compress,
            final CompressionCodec codec, final Progressable progress) throws IOException {
        super(conf, fs, dirName, comparator, valClass, compress, codec, progress);
        this.fs = fs;
        this.dir = new Path(dirName);
        initBloomFilter(conf);
    }

    @SuppressWarnings("rawtypes")
    public BloomMapFileWriter(final Configuration conf, final FileSystem fs, final String dirName,
            final WritableComparator comparator, final Class valClass, final CompressionType compress,
            final Progressable progress) throws IOException {
        super(conf, fs, dirName, comparator, valClass, compress, progress);
        this.fs = fs;
        this.dir = new Path(dirName);
        initBloomFilter(conf);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public synchronized void append(final WritableComparable key, final Writable val) throws IOException {
        super.append(key, val);

        buf.reset();
        key.write(buf);

        // patch from HADOOP-6546
        bloomKey.set(byteArrayForBloomKey(buf), 1.0);
        bloomFilter.add(bloomKey);
    }

    @Override
    public synchronized void close() throws IOException {
        super.close();
        DataOutputStream out = fs.create(new Path(dir, BloomMapFile.BLOOM_FILE_NAME), true);
        bloomFilter.write(out);
        out.flush();
        out.close();
    }

    private synchronized void initBloomFilter(final Configuration conf) {
        numKeys = conf.getInt("io.mapfile.bloom.size", 1024 * 1024);
        // vector size should be <code>-kn / (ln(1 - c^(1/k)))</code> bits for
        // single key, where <code> is the number of hash functions,
        // <code>n</code> is the number of keys and <code>c</code> is the desired
        // max. error rate.
        // Our desired error rate is by default 0.005, i.e. 0.5%
        float errorRate = conf.getFloat("io.mapfile.bloom.error.rate", 0.005f);
        vectorSize = (int) Math.ceil(-BloomMapFile.HASH_COUNT * numKeys
                / Math.log(1.0 - Math.pow(errorRate, 1.0 / BloomMapFile.HASH_COUNT)));
        bloomFilter = new DynamicBloomFilter(vectorSize, BloomMapFile.HASH_COUNT, Hash.getHashType(conf), numKeys);
    }

    /**
     * Patch from HADOOP-6546
     */
    public static byte[] byteArrayForBloomKey(final DataOutputBuffer buf) {

        int cleanLength = buf.getLength();
        byte[] ba = buf.getData();

        if (cleanLength != ba.length) {
            ba = new byte[cleanLength];
            System.arraycopy(buf.getData(), 0, ba, 0, cleanLength);
        }

        return ba;
    }
}
