package com.storebrand.scheduledtask;

/**
 * Retention policy for a scheduled task.
 */
public interface RetentionPolicy {
    int getDeleteRunsAfterDays();

    int getDeleteSuccessfulRunsAfterDays();

    int getDeleteFailedRunsAfterDays();

    int getKeepMaxFailedRuns();

    int getKeepMaxSuccessfulRuns();

    int getKeepMaxRuns();

    boolean isRetentionPolicyEnabled();
}
