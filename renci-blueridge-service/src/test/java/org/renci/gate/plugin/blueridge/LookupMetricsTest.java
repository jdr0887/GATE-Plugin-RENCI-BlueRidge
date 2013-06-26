package org.renci.gate.plugin.blueridge;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

import org.junit.Test;
import org.renci.gate.GlideinMetric;
import org.renci.jlrm.Queue;
import org.renci.jlrm.Site;
import org.renci.jlrm.pbs.PBSJobStatusInfo;
import org.renci.jlrm.pbs.PBSJobStatusType;
import org.renci.jlrm.pbs.ssh.PBSSSHJob;
import org.renci.jlrm.pbs.ssh.PBSSSHLookupStatusCallable;

public class LookupMetricsTest {

    @Test
    public void testLookupMetricsForReal() {
        Site site = new Site();
        site.setName("BlueRidge");
        site.setProject("RENCI");
        site.setUsername("mapseq");
        site.setSubmitHost("br0.renci.org");
        site.setMaxTotalPending(4);
        site.setMaxTotalRunning(4);

        Map<String, Queue> queueInfoMap = new HashMap<String, Queue>();

        Queue queue = new Queue();
        queue.setMaxJobLimit(10);
        queue.setMaxMultipleJobsToSubmit(2);
        queue.setName("serial");
        queue.setWeight(1D);
        queue.setPendingTime(1440);
        queue.setRunTime(5760);
        queueInfoMap.put("serial", queue);

        site.setQueueInfoMap(queueInfoMap);

        Map<String, GlideinMetric> metricsMap = new HashMap<String, GlideinMetric>();

        PBSSSHLookupStatusCallable callable = new PBSSSHLookupStatusCallable(site);
        Set<PBSJobStatusInfo> jobStatusSet = null;
        try {
            jobStatusSet = Executors.newSingleThreadExecutor().submit(callable).get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

        // get unique list of queues
        Set<String> queueSet = new HashSet<String>();
        if (jobStatusSet != null && jobStatusSet.size() > 0) {
            for (PBSJobStatusInfo info : jobStatusSet) {
                if (!queueSet.contains(info.getQueue())) {
                    queueSet.add(info.getQueue());
                }
            }

            for (PBSJobStatusInfo info : jobStatusSet) {
                if (metricsMap.containsKey(info.getQueue())) {
                    continue;
                }
                if (!"glidein".equals(info.getJobName())) {
                    continue;
                }
                metricsMap.put(info.getQueue(), new GlideinMetric(0, 0, info.getQueue()));
            }

            for (PBSJobStatusInfo info : jobStatusSet) {

                if (!"glidein".equals(info.getJobName())) {
                    continue;
                }

                switch (info.getType()) {
                    case QUEUED:
                        metricsMap.get(info.getQueue()).incrementPending();
                        break;
                    case RUNNING:
                        metricsMap.get(info.getQueue()).incrementRunning();
                        break;
                }
            }

        }

        for (String key : metricsMap.keySet()) {
            GlideinMetric metric = metricsMap.get(key);
            System.out.println(metric.toString());
        }

    }

    @Test
    public void testLookupMetrics() {
        List<PBSSSHJob> jobCache = new ArrayList<PBSSSHJob>();

        Set<PBSJobStatusInfo> jobStatusSet = new HashSet<PBSJobStatusInfo>();
        Random r = new Random();
        for (int i = 0; i < 30; ++i) {
            jobStatusSet.add(new PBSJobStatusInfo(r.nextInt() + "", PBSJobStatusType.RUNNING, "pseq_prod", "asdf"));
        }

        for (int i = 0; i < 10; ++i) {
            jobStatusSet.add(new PBSJobStatusInfo(r.nextInt() + "", PBSJobStatusType.QUEUED, "pseq_prod", "asdf"));
        }

        for (int i = 0; i < 20; ++i) {
            jobStatusSet.add(new PBSJobStatusInfo(r.nextInt() + "", PBSJobStatusType.RUNNING, "week", "asdf"));
        }

        for (int i = 0; i < 6; ++i) {
            jobStatusSet.add(new PBSJobStatusInfo(r.nextInt() + "", PBSJobStatusType.QUEUED, "week", "asdf"));
        }

        // get unique list of queues
        Set<String> queueSet = new HashSet<String>();
        if (jobStatusSet != null && jobStatusSet.size() > 0) {
            for (PBSJobStatusInfo info : jobStatusSet) {
                queueSet.add(info.getQueue());
            }
            for (PBSSSHJob job : jobCache) {
                queueSet.add(job.getQueueName());
            }
        }

        Set<String> alreadyTalliedJobIdSet = new HashSet<String>();
        Map<String, GlideinMetric> jobTallyMap = new HashMap<String, GlideinMetric>();

        if (jobStatusSet != null && jobStatusSet.size() > 0) {
            for (PBSJobStatusInfo info : jobStatusSet) {
                if (!jobTallyMap.containsKey(info.getQueue())) {
                    jobTallyMap.put(info.getQueue(), new GlideinMetric(0, 0, info.getQueue()));
                }
                alreadyTalliedJobIdSet.add(info.getJobId());
            }

            for (PBSJobStatusInfo info : jobStatusSet) {
                GlideinMetric metric = jobTallyMap.get(info.getQueue());
                switch (info.getType()) {
                    case QUEUED:
                        metric.setPending(metric.getPending() + 1);
                        break;
                    case RUNNING:
                        metric.setRunning(metric.getRunning() + 1);
                        break;
                }
            }
        }

        PBSSSHJob job = new PBSSSHJob("test", new File("/bin/hostname"));
        job.setId(r.nextInt() + "");
        job.setQueueName("pseq_prod");
        jobCache.add(job);

        Iterator<PBSSSHJob> jobCacheIter = jobCache.iterator();
        while (jobCacheIter.hasNext()) {
            PBSSSHJob nextJob = jobCacheIter.next();
            for (PBSJobStatusInfo info : jobStatusSet) {
                if (!alreadyTalliedJobIdSet.contains(nextJob.getId()) && nextJob.getId().equals(info.getJobId())) {
                    GlideinMetric metric = jobTallyMap.get(info.getQueue());
                    switch (info.getType()) {
                        case QUEUED:
                            metric.setPending(metric.getPending() + 1);
                            break;
                        case RUNNING:
                            metric.setRunning(metric.getRunning() + 1);
                            break;
                        case COMPLETE:
                            jobCacheIter.remove();
                            break;
                        case SUSPENDED:
                        default:
                            break;
                    }
                }
            }
        }

        int totalRunningGlideinJobs = 0;
        int totalPendingGlideinJobs = 0;

        for (String queue : queueSet) {
            GlideinMetric metrics = jobTallyMap.get(queue);
            totalRunningGlideinJobs += metrics.getRunning();
            totalPendingGlideinJobs += metrics.getPending();
        }

        int totalSiteJobs = totalRunningGlideinJobs + totalPendingGlideinJobs;
        assertTrue(totalSiteJobs == 66);

    }
}
