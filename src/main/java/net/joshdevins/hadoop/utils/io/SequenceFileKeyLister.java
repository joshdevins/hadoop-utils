package net.joshdevins.hadoop.utils.io;

import java.io.PrintStream;
import java.net.URI;

import net.joshdevins.hadoop.utils.ExitException;
import net.joshdevins.hadoop.utils.MainUtils;

import org.apache.commons.lang.Validate;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;

/**
 * Simple runner to list all keys in a {@link SequenceFile}, regardless of key type.
 * 
 * @author Josh Devins
 */
public class SequenceFileKeyLister extends Configured implements Tool {

    private final PrintStream out;

    /**
     * Default constructor.
     */
    public SequenceFileKeyLister() {
        this(System.out);
    }

    /**
     * Test injection constructor.
     */
    public SequenceFileKeyLister(final PrintStream out) {
        super();
        this.out = out;
    }

    @Override
    public int run(final String[] args) throws Exception {

        try {
            Validate.notNull(args);
            Validate.isTrue(args.length == 1);

            Validate.notEmpty(args[0]);

        } catch (IllegalArgumentException iae) {

            System.err.printf("Usage: %s [generic options] <input>\n", SequenceFileKeyLister.class.getSimpleName()); // NOPMD
            GenericOptionsParser.printGenericCommandUsage(System.err);
            throw new ExitException("Illegal argument expcetion");
        }

        String input = args[0];

        SequenceFile.Reader reader = null;
        try {
            reader = new SequenceFile.Reader(FileSystem.get(URI.create(input), getConf()), new Path(input), getConf());
            Writable key = (Writable) reader.getKeyClass().newInstance();
            Writable value = (Writable) reader.getValueClass().newInstance();

            while (reader.next(key, value)) {
                out.println(key.toString());
            }

        } catch (IllegalAccessException iae) {
            throw new IllegalStateException("Error instantiating key or value class", iae);

        } catch (InstantiationException ie) {
            throw new IllegalStateException("Error instantiating key or value class", ie);

        } finally {
            IOUtils.closeStream(reader);
        }

        return 0;
    }

    public static void main(final String[] args) throws Exception {
        MainUtils.toolRunner(new SequenceFileKeyLister(), args);
    }
}
