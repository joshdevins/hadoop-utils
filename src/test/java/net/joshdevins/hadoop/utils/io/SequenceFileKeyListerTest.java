package net.joshdevins.hadoop.utils.io;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import junit.framework.Assert;
import net.joshdevins.hadoop.utils.MainUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

public class SequenceFileKeyListerTest {

    private static final String TEST_SEQUENCE_FILE = "target/test/output/SequenceFileKeyListerTest/file.seq";

    private static final int TEST_SIZE = 10;

    @Before
    public void before() throws Exception {

        Configuration conf = new Configuration();
        SequenceFile.Writer writer = SequenceFile.createWriter(FileSystem.newInstance(conf), conf, new Path(
                TEST_SEQUENCE_FILE), Text.class, Text.class);

        for (int i = 0; i < TEST_SIZE; i++) {
            writer.append(new Text("Key: " + i), new Text("Value: " + i));
        }

        IOUtils.closeStream(writer);
    }

    @Test
    public void testLister() throws Exception {

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        MainUtils.toolRunnerWithoutExit(new SequenceFileKeyLister(new PrintStream(baos)),
                new String[] { TEST_SEQUENCE_FILE });

        String[] split = baos.toString().split("\n");
        Assert.assertNotNull(split);
        Assert.assertEquals(TEST_SIZE, split.length);

        for (int i = TEST_SIZE - 1; i >= 0; i--) {
            Assert.assertEquals("Key: " + i, split[i]);
        }
    }
}
