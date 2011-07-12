package net.joshdevins.hadoop.utils.io.converter;

import java.io.IOException;

import net.joshdevins.hadoop.utils.MainUtils;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;

/**
 * Writes files in a directory to a {@link SequenceFile}.
 * 
 * @see AbstractFilesIntoHdfsFile
 * 
 * @author Josh Devins
 */
public final class FilesIntoSequenceFile extends AbstractFilesIntoHdfsFile<SequenceFile.Writer> {

    @Override
    protected void appendFilenameAndBytesToWriter(final String key, final byte[] bytes, final SequenceFile.Writer writer)
            throws IOException {

        writer.append(new Text(key), new BytesWritable(bytes));
    }

    @Override
    protected SequenceFile.Writer createWriter(final FileSystem outputFS) throws IOException {

        return SequenceFile.createWriter(outputFS, getConf(), new Path(getOutput()), Text.class, BytesWritable.class,
                CompressionType.NONE);
    }

    @Override
    protected Class<?> getImplClass() {
        return FilesIntoSequenceFile.class;
    }

    public static void main(final String[] args) throws Exception {
        MainUtils.toolRunner(new FilesIntoSequenceFile(), args);
    }
}
