package net.joshdevins.hadoop.utils.io.converter;

import java.io.IOException;

import net.joshdevins.hadoop.utils.MainUtils;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;

/**
 * Writes files in a directory to a {@link MapFile}.
 * 
 * @see AbstractFilesIntoHdfsFile
 * 
 * @author Josh Devins
 */
public final class FilesIntoMapFile extends AbstractFilesIntoHdfsFile<MapFile.Writer> {

    @Override
    protected void appendFilenameAndBytesToWriter(final String key, final byte[] bytes, final MapFile.Writer writer)
            throws IOException {

        writer.append(new Text(key), new BytesWritable(bytes));
    }

    @Override
    protected MapFile.Writer createWriter(final FileSystem outputFS) throws IOException {

        return new MapFile.Writer(getConf(), outputFS, getOutput(), Text.class, BytesWritable.class,
                CompressionType.NONE);
    }

    @Override
    protected Class<?> getImplClass() {
        return FilesIntoMapFile.class;
    }

    public static void main(final String[] args) throws Exception {
        MainUtils.toolRunner(new FilesIntoMapFile(), args);
    }
}
