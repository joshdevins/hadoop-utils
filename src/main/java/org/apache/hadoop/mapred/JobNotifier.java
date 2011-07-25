package org.apache.hadoop.mapred;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.Tool;

/**
 * Small daemon to watch jobs, allow subscriptions by username, and sends out notifications when jobs complete (success
 * or failure).
 * 
 * @author Josh Devins
 */
public class JobNotifier extends Configured implements Tool {

    @SuppressWarnings("deprecation")
    @Override
    public int run(final String[] args) throws Exception {

        JobConf jobConf = new JobConf(getConf());
        JobClient client = new JobClient(jobConf);

        String tracker = getConf().get("mapred.job.tracker", "local");

        JobSubmissionProtocol jobSubmitClient;
        if ("local".equals(tracker)) {
            jobSubmitClient = new LocalJobRunner(jobConf);
        } else {
            jobSubmitClient = createRPCProxy(JobTracker.getAddress(getConf()), getConf());
        }

        JobStatus[] jobs = jobSubmitClient.jobsToComplete();
        for (JobStatus job : jobs) {
            if ("devins".equals(job.getUsername())) {
                job.getRunState();
            }
        }

        return 0;
    }

    private JobSubmissionProtocol createRPCProxy(final InetSocketAddress addr, final Configuration conf)
            throws IOException {

        return (JobSubmissionProtocol) RPC.getProxy(JobSubmissionProtocol.class, JobSubmissionProtocol.versionID, addr,
                conf, NetUtils.getSocketFactory(conf, JobSubmissionProtocol.class));
    }

    public static void main(final String[] args) {

    }
}
