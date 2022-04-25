package com.storebrand.scheduledtask;

import com.storebrand.scheduledtask.ScheduledTask.Criticality;
import com.storebrand.scheduledtask.ScheduledTask.Recovery;
import com.storebrand.scheduledtask.ScheduledTask.RetentionPolicy;

/**
 * Configuration for a scheduled task.
 *
 * @author Kristian Hiim
 */
public class ScheduledTaskConfig {

    private final String _name;
    private final String _cronExpression;
    private final int _maxExpectedMinutesToRun;
    private final Criticality _criticality;
    private final Recovery _recovery;
    private final RetentionPolicy _retentionPolicy;

    public ScheduledTaskConfig(String name, String cronExpression, int maxExpectedMinutesToRun,
            Criticality criticality, Recovery recovery) {
        this(name, cronExpression, maxExpectedMinutesToRun, criticality, recovery,
                StaticRetentionPolicy.DEFAULT_RETENTION_POLICY);
    }

    public ScheduledTaskConfig(String name, String cronExpression, int maxExpectedMinutesToRun,
            Criticality criticality, Recovery recovery, RetentionPolicy retentionPolicy) {
        _name = name;
        _cronExpression = cronExpression;
        _maxExpectedMinutesToRun = maxExpectedMinutesToRun;
        _criticality = criticality;
        _recovery = recovery;
        _retentionPolicy = retentionPolicy;
    }

    public String getName() {
        return _name;
    }

    public String getCronExpression() {
        return _cronExpression;
    }

    public int getMaxExpectedMinutesToRun() {
        return _maxExpectedMinutesToRun;
    }

    public Criticality getCriticality() {
        return _criticality;
    }

    public Recovery getRecovery() {
        return _recovery;
    }

    public RetentionPolicy getRetentionPolicy() {
        return _retentionPolicy;
    }

    /**
     * Retention policy for a scheduled task. Use {@link Builder} to create a scheduled task. The default retention
     * policy {@link #DEFAULT_RETENTION_POLICY} is to keep logs for 365 days.
     * <p>
     * Keep max days can be set for successful, failed and a general keep max. These determine the absolute maximum
     * amount of time a task run will be kept, with the general keep max overriding any other setting here. Setting any
     * of the values to 0 will disable this. You should not set all these to 0, unless you define a maximum
     * number of runs to keep with the "keep last" settings.
     * <p>
     * The keep last settings works similar to keep max days, but they instead define a set amount of runs to keep,
     * instead of a maximum amount of days. "Keep last total" will override any other setting, and will not allow more
     * than this amount of runs to be kept.
     */
    public static class StaticRetentionPolicy implements RetentionPolicy {

        public static final RetentionPolicy DEFAULT_RETENTION_POLICY = new Builder().build();

        private final int _keepMaxFailedRuns;
        private final int _keepMaxSuccessfulRuns;

        private final int _keepMaxRuns;

        private final int _deleteFailedRunsAfterDays;
        private final int _deleteSuccessfulRunsAfterDays;

        private final int _deleteRunsAfterDays;

        private StaticRetentionPolicy(int keepMaxSuccessfulRuns, int keepMaxFailedRuns, int keepMaxRuns, int deleteSuccessfulRunsAfterDays,
                int deleteFailedRunsAfterDays, int deleteRunsAfterDays) {
            _keepMaxFailedRuns = keepMaxFailedRuns;
            _keepMaxSuccessfulRuns = keepMaxSuccessfulRuns;
            _keepMaxRuns = keepMaxRuns;
            _deleteFailedRunsAfterDays = deleteFailedRunsAfterDays;
            _deleteSuccessfulRunsAfterDays = deleteSuccessfulRunsAfterDays;
            _deleteRunsAfterDays = deleteRunsAfterDays;
        }

        public static RetentionPolicy keepMaxDays(int keepMaxDays) {
            return new Builder().deleteRunsAfterDays(keepMaxDays).build();
        }

        @Override
        public int getKeepMaxFailedRuns() {
            return _keepMaxFailedRuns;
        }

        @Override
        public int getKeepMaxSuccessfulRuns() {
            return _keepMaxSuccessfulRuns;
        }

        @Override
        public int getKeepMaxRuns() {
            return _keepMaxRuns;
        }

        @Override
        public int getDeleteFailedRunsAfterDays() {
            return _deleteFailedRunsAfterDays;
        }

        @Override
        public int getDeleteSuccessfulRunsAfterDays() {
            return _deleteSuccessfulRunsAfterDays;
        }

        @Override
        public int getDeleteRunsAfterDays() {
            return _deleteRunsAfterDays;
        }

        @Override
        public boolean isRetentionPolicyEnabled() {
            return _deleteRunsAfterDays > 0 || _deleteFailedRunsAfterDays > 0 || _deleteSuccessfulRunsAfterDays > 0
                    || _keepMaxRuns > 0 || _keepMaxFailedRuns > 0 || _keepMaxSuccessfulRuns > 0;
        }

        public static class Builder {
            private int _keepMaxSuccessfulRuns;
            private int _keepMaxFailedRuns;

            private int _keepMaxRuns;

            private int _deleteSuccessfulRunsAfterDays;
            private int _deleteFailedRunsAfterDays;

            private int _deleteRunsAfterDays = ScheduledTaskBuilder.DEFAULT_DELETE_RUNS_AFTER_DAYS; // Default one year

            public Builder keepMaxSuccessfulRuns(int keepMaxSuccessfulRuns) {
                _keepMaxSuccessfulRuns = keepMaxSuccessfulRuns;
                return this;
            }

            public Builder keepMaxFailedRuns(int keepMaxFailedRuns) {
                _keepMaxFailedRuns = keepMaxFailedRuns;
                return this;
            }

            public Builder keepMaxRuns(int keepMaxRuns) {
                _keepMaxRuns = keepMaxRuns;
                return this;
            }

            public Builder deleteSuccessfulRunsAfterDays(int days) {
                _deleteSuccessfulRunsAfterDays = days;
                return this;
            }

            public Builder deleteFailedRunsAfterDays(int days) {
                _deleteFailedRunsAfterDays = days;
                return this;
            }

            public Builder deleteRunsAfterDays(int days) {
                _deleteRunsAfterDays = days;
                return this;
            }

            public RetentionPolicy build() {
                return new StaticRetentionPolicy(_keepMaxSuccessfulRuns, _keepMaxFailedRuns, _keepMaxRuns,
                        _deleteSuccessfulRunsAfterDays, _deleteFailedRunsAfterDays, _deleteRunsAfterDays);
            }
        }
    }

}
