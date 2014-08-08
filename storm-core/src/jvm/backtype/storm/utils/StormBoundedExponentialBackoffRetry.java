/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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

    /**
     * The class provides generic backoff retry strategy for storm.
     * The class overrides curator frameworks implementation of
     * BoundedExponentialBackOffRetry to overcome following short-comings:
     * - BoundedExponentialBackOffRetry implementation limits maxRetries to 29.
     * - BoundedExponentialBackOffRetry implementation of getSleepTimeMs fails
     *   to check if retryCount > 29 potentially passing -ve argument
     *   to random.nextInt.
     * - Also, exponential calculation in quickly catches up to maxSleepTime
     * and not consistently increasing sleep
     * This class calculates threshold for exponentially increases sleeptime
     * for retries. Beyond threshold, the sleep time increase is linear.
     * Also adding jitter within range for either exponential or linear part
     * of the retry.
     */

    public StormBoundedExponentialBackoffRetry(int baseSleepTimeMs, int maxSleepTimeMs, int maxRetries) {
        super(baseSleepTimeMs, maxSleepTimeMs, maxRetries);
        expRetriesThreshold = 1;
        while ((1 << (expRetriesThreshold + 1)) < ((maxSleepTimeMs - baseSleepTimeMs) / 2))
            expRetriesThreshold++;
        LOG.info("The baseSleepTimeMs [" + baseSleepTimeMs + "] the maxSleepTimeMs [" + maxSleepTimeMs + "] " +
                "the maxRetries [" + maxRetries + "]");
        if (baseSleepTimeMs > maxSleepTimeMs) {
            LOG.warn("Misconfiguration: the baseSleepTimeMs [" + baseSleepTimeMs + "] can't be greater than " +
                    "the maxSleepTimeMs [" + maxSleepTimeMs + "].");
        }
        this.stepSize = Math.max(1, (maxSleepTimeMs - (1 << expRetriesThreshold)) / (maxRetries - expRetriesThreshold));
        this.linearBaseSleepMs = super.getBaseSleepTimeMs() + (1 << expRetriesThreshold);
    }

    @Override
    public int getSleepTimeMs(int retryCount, long elapsedTimeMs) {
        if (retryCount < expRetriesThreshold) {
            int exp = 1 << retryCount;
            int jitter = random.nextInt(exp);
            int sleepTimeMs = super.getBaseSleepTimeMs() + exp + jitter;
            return sleepTimeMs;
        } else {
            int stepJitter = random.nextInt(stepSize);
            return Math.min(super.getMaxSleepTimeMs(), (linearBaseSleepMs +
                    (stepSize * (retryCount - expRetriesThreshold)) + stepJitter));
        }
    }
}
