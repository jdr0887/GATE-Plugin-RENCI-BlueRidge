package org.renci.gate.plugin.blueridge;

import java.util.Map;

import org.renci.gate.AbstractGATEService;
import org.renci.gate.GlideinMetric;
import org.renci.jlrm.Queue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author jdr0887
 */
public class BlueRidgeGATEService extends AbstractGATEService {

    private final Logger logger = LoggerFactory.getLogger(BlueRidgeGATEService.class);

    public BlueRidgeGATEService() {
        super();
    }

    @Override
    public Map<String, GlideinMetric> lookupMetrics() {
        return null;
    }

    @Override
    public void createGlidein(Queue queue) {
        logger.info("ENTERING createGlidein(Queue)");
        if (!getActiveQueues().contains(queue.getName())) {
            logger.warn("queue name is not in active queue list...see etc/org.renci.gate.plugin.killdevil.cfg");
            return;
        }
    }

    @Override
    public void deleteGlidein(Queue queue) {
        logger.info("ENTERING deleteGlidein(Queue)");

    }

    @Override
    public void deletePendingGlideins() {
    }

}
