package net.joshdevins.hadoop.utils.io.converter;

import java.io.IOException;

import net.joshdevins.hadoop.utils.io.ExitException;

import org.apache.commons.lang.Validate;
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

    private static final String USAGE = "Usage: FilesIntoSequenceFile <input directory> <output SequenceFile>";

    public FilesIntoSequenceFile(final String input, final String output) {
        super(input, output, USAGE);
    }

    @Override
    protected void appendFilenameAndBytesToWriter(final String key, final byte[] bytes, final SequenceFile.Writer writer)
            throws IOException {

        writer.append(new Text(key), new BytesWritable(bytes));
    }

    @Override
    protected SequenceFile.Writer createWriter(final FileSystem outputFS) throws IOException {

        return SequenceFile.createWriter(outputFS, getConfig(), new Path(getOutput()), Text.class, BytesWritable.class,
                CompressionType.NONE);
    }

    public static void main(final String[] args) {

        Validate.notNull(args, USAGE);
        Validate.isTrue(args.length == 2, USAGE);

        try {
            FilesIntoSequenceFile runner = new FilesIntoSequenceFile(args[0], args[1]);
            runner.run();
        } catch (ExitException ee) {
            // abnormal exit
            System.exit(1);
        }
    }
}
