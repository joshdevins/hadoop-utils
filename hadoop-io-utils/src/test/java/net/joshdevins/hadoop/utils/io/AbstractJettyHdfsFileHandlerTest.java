package net.joshdevins.hadoop.utils.io;

import org.junit.Assert;
import org.junit.Test;

public class AbstractJettyHdfsFileHandlerTest {

    @Test
    public void testGetMimeType() {

        // unknown types
        Assert.assertEquals("application/octet-stream", AbstractJettyHdfsFileHandler.getMimeType("file"));

        // known types - text
        Assert.assertEquals("text/plain", AbstractJettyHdfsFileHandler.getMimeType("file.txt"));
        Assert.assertEquals("text/html", AbstractJettyHdfsFileHandler.getMimeType("file.html"));

        // known types - images
        Assert.assertEquals("image/jpeg", AbstractJettyHdfsFileHandler.getMimeType("file.jpg"));
        Assert.assertEquals("image/png", AbstractJettyHdfsFileHandler.getMimeType("file.png"));
        Assert.assertEquals("image/gif", AbstractJettyHdfsFileHandler.getMimeType("file.gif"));
    }
}
