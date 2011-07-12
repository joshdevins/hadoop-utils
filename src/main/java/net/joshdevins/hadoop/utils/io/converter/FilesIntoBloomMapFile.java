package net.joshdevins.hadoop.utils.io.converter;

import java.io.IOException;

import net.joshdevins.hadoop.utils.MainUtils;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.BloomMapFile;
import org.apache.hadoop.io.BloomMapFileWriter;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;

/**
 * Writes files in a directory to a {@link BloomMapFile}.
 * 
 * @see AbstractFilesIntoHdfsFile
 * 
 * @author Josh Devins
 */
public final class FilesIntoBloomMapFile extends AbstractFilesIntoHdfsFile<BloomMapFileWriter> {

    private static final String USAGE = "Usage: FilesIntoBloomMapFile <input directory> <output BloomMapFile>";

    @Override
    protected void appendFilenameAndBytesToWriter(final String key, final byte[] bytes, final BloomMapFileWriter writer)
            throws IOException {

        writer.append(new Text(key), new BytesWritable(bytes));
    }

    @Override
    protected BloomMapFileWriter createWriter(final FileSystem outputFS) throws IOException {

        return new BloomMapFileWriter(getConf(), outputFS, getOutput(), Text.class, BytesWritable.class,
                CompressionType.NONE);
    }

    @Override
    protected String getUsage() {
        return USAGE;
    }

    public static void main(final String[] args) throws Exception {
        MainUtils.toolRunner(new FilesIntoBloomMapFile(), args);
    }
}
