package backtype.storm.utils;

import org.apache.curator.retry.BoundedExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

public class StormBoundedExponentialBackoffRetry extends BoundedExponentialBackoffRetry {
    private static final Logger LOG = LoggerFactory.getLogger(StormBoundedExponentialBackoffRetry.class);
    private int stepSize;
    private int expRetriesThreshold;
    private final Random random = new Random();
    private final int linearBaseSleepMs;

    public StormBoundedExponentialBackoffRetry(int baseSleepTimeMs, int maxSleepTimeMs, int maxRetries) {
        super(baseSleepTimeMs, maxSleepTimeMs, maxRetries);
        expRetriesThreshold = 1;
        while( (1 << (expRetriesThreshold +1)) < ((maxSleepTimeMs - baseSleepTimeMs) /2 ))
            expRetriesThreshold++;
        LOG.info("The baseSleepTimeMs [" + baseSleepTimeMs + "] the maxSleepTimeMs [" + maxSleepTimeMs + "] the maxRetries [" + maxRetries +"]");
        if(baseSleepTimeMs > maxSleepTimeMs) {
            LOG.warn("Misconfiguration: the baseSleepTimeMs [" + baseSleepTimeMs + "] can't be greater than the maxSleepTimeMs [" + maxSleepTimeMs + "].");
        }
        this.stepSize = Math.max(1,(maxSleepTimeMs - (1 << expRetriesThreshold)) / (maxRetries - expRetriesThreshold));
        this.linearBaseSleepMs = super.getBaseSleepTimeMs() + (1 << expRetriesThreshold);
    }

    @Override
    public int getSleepTimeMs(int retryCount, long elapsedTimeMs) {
        if(retryCount < expRetriesThreshold) {
            int exp = 1 << retryCount;
            int jitter = random.nextInt(exp);
            int sleepTimeMs = super.getBaseSleepTimeMs() + exp + jitter;
            return sleepTimeMs;
        }
        else {
            int stepJitter = random.nextInt(stepSize);
            return Math.min(super.getMaxSleepTimeMs(), (linearBaseSleepMs + (stepSize * (retryCount - expRetriesThreshold)) + stepJitter));
        }
    }
}
