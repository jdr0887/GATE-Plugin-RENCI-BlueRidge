package org.renci.gate.plugin.blueridge;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;

import org.apache.commons.lang.StringUtils;
import org.renci.gate.AbstractGATEService;
import org.renci.gate.GATEException;
import org.renci.gate.GlideinMetric;
import org.renci.jlrm.JLRMException;
import org.renci.jlrm.Queue;
import org.renci.jlrm.commons.ssh.SSHConnectionUtil;
import org.renci.jlrm.pbs.PBSJobStatusInfo;
import org.renci.jlrm.pbs.PBSJobStatusType;
import org.renci.jlrm.pbs.ssh.PBSSSHKillCallable;
import org.renci.jlrm.pbs.ssh.PBSSSHLookupStatusCallable;
import org.renci.jlrm.pbs.ssh.PBSSSHSubmitCondorGlideinCallable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author jdr0887
 */
public class BlueRidgeGATEService extends AbstractGATEService {

    private final Logger logger = LoggerFactory.getLogger(BlueRidgeGATEService.class);

    private String username;

    public BlueRidgeGATEService() {
        super();
    }

    @Override
    public Boolean isValid() throws GATEException {
        logger.info("ENTERING isValid()");
        try {
            String results = SSHConnectionUtil.execute("ls /projects/mapseq/ | wc -l", getSite().getUsername(),
                    getSite().getSubmitHost());
            if (StringUtils.isNotEmpty(results) && Integer.valueOf(results.trim()) > 0) {
                return true;
            }
        } catch (NumberFormatException | JLRMException e) {
            throw new GATEException(e);
        }
        return false;
    }

    @Override
    public Map<String, GlideinMetric> lookupMetrics() throws GATEException {
        logger.info("ENTERING lookupMetrics()");
        Map<String, GlideinMetric> metricsMap = new HashMap<String, GlideinMetric>();

        try {
            PBSSSHLookupStatusCallable callable = new PBSSSHLookupStatusCallable(getSite());
            Set<PBSJobStatusInfo> jobStatusSet = Executors.newSingleThreadExecutor().submit(callable).get();
            logger.debug("jobStatusSet.size(): {}", jobStatusSet.size());

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

        } catch (Exception e) {
            throw new GATEException(e);
        }

        return metricsMap;
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

        try {
            logger.info("siteInfo: {}", getSite());
            logger.info("queueInfo: {}", queue);
            String hostAllow = "*.unc.edu";
            PBSSSHSubmitCondorGlideinCallable callable = new PBSSSHSubmitCondorGlideinCallable();
            callable.setCollectorHost(getCollectorHost());
            callable.setUsername(System.getProperty("user.name"));
            callable.setSite(getSite());
            callable.setJobName("glidein");
            callable.setQueue(queue);
            callable.setSubmitDir(submitDir);
            callable.setRequiredMemory(40);
            callable.setHostAllowRead(hostAllow);
            callable.setHostAllowWrite(hostAllow);

            Executors.newSingleThreadExecutor().submit(callable).get();

        } catch (Exception e) {
            throw new GATEException(e);
        }
    }

    @Override
    public void deleteGlidein(Queue queue) throws GATEException {
        logger.info("ENTERING deleteGlidein(Queue)");
        try {
            PBSSSHLookupStatusCallable lookupStatusCallable = new PBSSSHLookupStatusCallable(getSite());
            Set<PBSJobStatusInfo> jobStatusSet = Executors.newSingleThreadExecutor().submit(lookupStatusCallable).get();
            PBSSSHKillCallable killCallable = new PBSSSHKillCallable(getSite(), jobStatusSet.iterator().next()
                    .getJobId());
            Executors.newSingleThreadExecutor().submit(killCallable).get();
        } catch (Exception e) {
            throw new GATEException(e);
        }
    }

    @Override
    public void deletePendingGlideins() throws GATEException {
        logger.info("ENTERING deletePendingGlideins()");
        try {
            PBSSSHLookupStatusCallable lookupStatusCallable = new PBSSSHLookupStatusCallable(getSite());
            Set<PBSJobStatusInfo> jobStatusSet = Executors.newSingleThreadExecutor().submit(lookupStatusCallable).get();
            for (PBSJobStatusInfo info : jobStatusSet) {
                if (info.getType().equals(PBSJobStatusType.QUEUED)) {
                    PBSSSHKillCallable killCallable = new PBSSSHKillCallable(getSite(), info.getJobId());
                    Executors.newSingleThreadExecutor().submit(killCallable).get();
                }
                // throttle the deleteGlidein calls such that SSH doesn't complain
                Thread.sleep(2000);
            }
        } catch (Exception e) {
            throw new GATEException(e);
        }
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

}
