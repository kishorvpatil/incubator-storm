package backtype.storm.utils;

import junit.framework.TestCase;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StormBoundedExponentialBackoffRetryTest extends TestCase {
    private static final Logger LOG = LoggerFactory.getLogger(StormBoundedExponentialBackoffRetryTest.class);

    @Test
    public void testExponentialSleepLargeRetries() throws Exception {
        int baseSleepMs = 10;
        int maxSleepMs = 1000;
        int maxRetries = 900;
        validateSleepTimes(baseSleepMs, maxSleepMs, maxRetries);

    }

    @Test
    public void testExponentialSleep() throws Exception {
        int baseSleepMs = 10;
        int maxSleepMs = 100;
        int maxRetries = 40;
        validateSleepTimes(baseSleepMs, maxSleepMs, maxRetries);
    }

    @Test
    public void testExponentialSleepZeroMaxTries() throws Exception {
        int baseSleepMs = 10;
        int maxSleepMs = 100;
        int maxRetries = 0;
        validateSleepTimes(baseSleepMs, maxSleepMs, maxRetries);
    }

    @Test
    public void testExponentialSleepOneMaxTries() throws Exception {
        int baseSleepMs = 10;
        int maxSleepMs = 100;
        int maxRetries = 10;
        validateSleepTimes(baseSleepMs, maxSleepMs, maxRetries);
    }

    private void validateSleepTimes(int baseSleepMs, int maxSleepMs, int maxRetries) {
        StormBoundedExponentialBackoffRetry retryPolicy = new StormBoundedExponentialBackoffRetry(baseSleepMs, maxSleepMs, maxRetries);
        int retryCount = 0;
        int prevSleepMs = 0;
        LOG.info("The baseSleepMs [" + baseSleepMs + "] the maxSleepMs [" + maxSleepMs + "] the maxRetries [" + maxRetries + "]");
        while (retryCount <= maxRetries) {
            int currSleepMs = retryPolicy.getSleepTimeMs(retryCount, 0);
            LOG.info("For retryCount [" + retryCount + "] the previousSleepMs [" + prevSleepMs + "] the currentSleepMs [" + currSleepMs + "]");
            assertTrue("For retryCount [" + retryCount + "] the previousSleepMs [" + prevSleepMs + "] is not less than currentSleepMs [" + currSleepMs + "]", (prevSleepMs < currSleepMs) || (currSleepMs == maxSleepMs));
            assertTrue("For retryCount [" + retryCount + "] the currentSleepMs [" + currSleepMs + "] is less than baseSleepMs [" + baseSleepMs + "].", (baseSleepMs <= currSleepMs) || (currSleepMs == maxSleepMs));
            assertTrue("For retryCount [" + retryCount + "] the currentSleepMs [" + currSleepMs + "] is greater than maxSleepMs [" + maxSleepMs + "]", maxSleepMs >= currSleepMs);
            prevSleepMs = currSleepMs;
            retryCount++;
        }
    }
}

