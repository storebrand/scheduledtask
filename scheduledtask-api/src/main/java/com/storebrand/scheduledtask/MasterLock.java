package com.storebrand.scheduledtask;

import java.time.Instant;

/**
 * Information about the current master lock for scheduled tasks.
 */
public interface MasterLock {
    String getLockName();

    String getNodeName();

    Instant getLockTakenTime();

    Instant getLockLastUpdatedTime();

    /**
     * Check if this lock is still valid. If it is over 5 min old it is invalid meaning this host where the one to have
     * it last. The lock can't be re-claimed before it has passed 10 min since last update.
     */
    boolean isValid(Instant now);
}
