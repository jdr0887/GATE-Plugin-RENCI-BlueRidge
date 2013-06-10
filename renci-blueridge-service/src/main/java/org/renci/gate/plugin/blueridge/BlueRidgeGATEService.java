package org.renci.gate.plugin.blueridge;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

import org.apache.commons.lang.StringUtils;
import org.renci.gate.AbstractGATEService;
import org.renci.gate.GATEException;
import org.renci.gate.GlideinMetric;
import org.renci.jlrm.Queue;
import org.renci.jlrm.pbs.ssh.PBSSSHJob;
import org.renci.jlrm.pbs.ssh.PBSSSHKillCallable;
import org.renci.jlrm.pbs.ssh.PBSSSHSubmitCondorGlideinCallable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author jdr0887
 */
public class BlueRidgeGATEService extends AbstractGATEService {

    private final Logger logger = LoggerFactory.getLogger(BlueRidgeGATEService.class);

    private final List<PBSSSHJob> jobCache = new ArrayList<PBSSSHJob>();

    private String username;

    public BlueRidgeGATEService() {
        super();
    }

    @Override
    public Map<String, GlideinMetric> lookupMetrics() throws GATEException {
        return null;
    }

    @Override
    public void createGlidein(Queue queue) throws GATEException {
        logger.info("ENTERING createGlidein(Queue)");

        if (StringUtils.isNotEmpty(getActiveQueues()) && !getActiveQueues().contains(queue.getName())) {
            logger.warn("queue name is not in active queue list...see etc/org.renci.gate.plugin.kure.cfg");
            return;
        }

        File submitDir = new File("/tmp", System.getProperty("user.name"));
        submitDir.mkdirs();
        PBSSSHJob job = null;

        try {
            logger.info("siteInfo: {}", getSite());
            logger.info("queueInfo: {}", queue);
            String hostAllow = "*.unc.edu";
            PBSSSHSubmitCondorGlideinCallable callable = new PBSSSHSubmitCondorGlideinCallable();
            callable.setCollectorHost(getCollectorHost());
            // callable.setUsername(System.getProperty("user.name"));
            callable.setSite(getSite());
            callable.setJobName("glidein");
            callable.setQueue(queue);
            callable.setSubmitDir(submitDir);
            callable.setRequiredMemory(40);
            callable.setHostAllowRead(hostAllow);
            callable.setHostAllowWrite(hostAllow);

            job = Executors.newSingleThreadExecutor().submit(callable).get();
            if (job != null && StringUtils.isNotEmpty(job.getId())) {
                logger.info("job.getId(): {}", job.getId());
                jobCache.add(job);
            }
        } catch (Exception e) {
            throw new GATEException(e);
        }
    }

    @Override
    public void deleteGlidein(Queue queue) throws GATEException {
        logger.info("ENTERING deleteGlidein(Queue)");
        if (jobCache.size() > 0) {
            try {
                logger.info("siteInfo: {}", getSite());
                logger.info("queueInfo: {}", queue);
                PBSSSHJob job = jobCache.get(0);
                logger.info("job: {}", job.toString());
                PBSSSHKillCallable callable = new PBSSSHKillCallable(getSite(), job.getId());
                Executors.newSingleThreadExecutor().submit(callable).get();
                jobCache.remove(0);
            } catch (Exception e) {
                throw new GATEException(e);
            }
        }

    }

    @Override
    public void deletePendingGlideins() throws GATEException {
        logger.info("ENTERING deletePendingGlideins()");
        // try {
        // PBSSSHLookupStatusCallable lookupStatusCallable = new PBSSSHLookupStatusCallable(jobCache, getSite());
        // Set<PBSJobStatusInfo> jobStatusSet = Executors.newSingleThreadExecutor().submit(lookupStatusCallable).get();
        // for (PBSJobStatusInfo info : jobStatusSet) {
        // if (info.getType().equals(PBSJobStatusType.PENDING)) {
        // PBSSSHKillCallable killCallable = new PBSSSHKillCallable(getSite(), info.getJobId());
        // Executors.newSingleThreadExecutor().submit(killCallable).get();
        // }
        // // throttle the deleteGlidein calls such that SSH doesn't complain
        // Thread.sleep(2000);
        // }
        // } catch (Exception e) {
        // throw new GATEException(e);
        // }
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

}
