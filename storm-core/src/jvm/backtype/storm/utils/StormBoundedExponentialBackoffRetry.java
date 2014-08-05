package backtype.storm.utils;

import org.apache.curator.retry.BoundedExponentialBackoffRetry;

public class StormBoundedExponentialBackoffRetry extends BoundedExponentialBackoffRetry {

    public StormBoundedExponentialBackoffRetry(int baseSleepTimeMs, int maxSleepTimeMs, int maxRetries) {
        super(baseSleepTimeMs, maxSleepTimeMs, maxRetries);
    }

    @Override
    public int getSleepTimeMs(int retryCount, long elapsedTimeMs) {
        return super.getSleepTimeMs(retryCount, elapsedTimeMs);
    }
}
