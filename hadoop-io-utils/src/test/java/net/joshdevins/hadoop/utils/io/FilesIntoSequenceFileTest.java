package net.joshdevins.hadoop.utils.io;

import java.io.IOException;
import java.net.URI;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

public class FilesIntoSequenceFileTest {

    private static final String TEST_OUTPUT = "target/test/output/FilesIntoSequenceFileTest/file.seq";

    @Test(expected = IllegalArgumentException.class)
    public void testBadArgs() {
        FilesIntoSequenceFile.run(null, null);
    }

    @Test
    public void testRun() throws IOException {

        FilesIntoSequenceFile.run("src/test/resources/input", TEST_OUTPUT);
        List<Pair<String, String>> entries = readSequenceFile(TEST_OUTPUT);

        Assert.assertNotNull(entries);
        Assert.assertEquals(3, entries.size());

        for (int i = 0; i < entries.size(); i++) {

            Pair<String, String> entry = entries.get(i);
            String key = entry.getA();
            String value = entry.getB();

            Assert.assertEquals(i + ".txt", key);
            Assert.assertEquals("Contents of file " + i, value);
        }
    }

    private List<Pair<String, String>> readSequenceFile(final String uri) throws IOException {

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        Path path = new Path(uri);
        SequenceFile.Reader reader = null;
        List<Pair<String, String>> list = new LinkedList<Pair<String, String>>();

        try {
            reader = new SequenceFile.Reader(fs, path, conf);
            Text key = new Text();
            BytesWritable value = new BytesWritable();

            while (reader.next(key, value)) {

                byte[] bytes = new byte[value.getLength()];
                System.arraycopy(value.getBytes(), 0, bytes, 0, value.getLength());

                list.add(new Pair<String, String>(key.toString(), new String(bytes)));
            }

        } finally {
            IOUtils.closeStream(reader);
        }

        return list;
    }
}
