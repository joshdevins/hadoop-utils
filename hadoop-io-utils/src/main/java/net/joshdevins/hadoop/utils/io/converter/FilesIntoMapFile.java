package net.joshdevins.hadoop.utils.io.converter;

import java.io.IOException;

import net.joshdevins.hadoop.utils.io.ExitException;

import org.apache.commons.lang.Validate;
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

    private static final String USAGE = "Usage: FilesIntoBloomMapFile <input directory> <output BloomMapFile>";

    public FilesIntoMapFile(final String input, final String output) {
        super(input, output, USAGE);
    }

    @Override
    protected void appendFilenameAndBytesToWriter(final String key, final byte[] bytes, final MapFile.Writer writer)
            throws IOException {

        writer.append(new Text(key), new BytesWritable(bytes));
    }

    @Override
    protected MapFile.Writer createWriter(final FileSystem outputFS) throws IOException {

        return new MapFile.Writer(getConfig(), outputFS, getOutput(), Text.class, BytesWritable.class,
                CompressionType.NONE);
    }

    public static void main(final String[] args) {

        Validate.notNull(args, USAGE);
        Validate.isTrue(args.length == 2, USAGE);

        try {
            FilesIntoMapFile runner = new FilesIntoMapFile(args[0], args[1]);
            runner.run();
        } catch (ExitException ee) {
            // abnormal exit
            System.exit(1);
        }
    }
}
