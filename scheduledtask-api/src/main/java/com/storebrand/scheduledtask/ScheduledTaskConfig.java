package com.storebrand.scheduledtask;

/**
 * Configuration for a scheduled task.
 * <p>
 * A scheduled task has a name, a default cron expression and a maximum expected minutes to run. In addition we define
 * the criticality of this scheduled task, and if it is self-healing or if we need manual intervention if something
 * goes wrong.
 * <p>
 * We can also define a custom {@link RetentionPolicy} for the scheduled task, that determines how long we will keep
 * record of the runs.
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
                RetentionPolicy.DEFAULT_RETENTION_POLICY);
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

    public enum Criticality {
        MISSION_CRITICAL,
        VITAL,
        IMPORTANT,
        MINOR
    }

    public enum Recovery {
        SELF_HEALING,
        MANUAL_INTERVENTION
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
    public static class RetentionPolicy {

        public static final RetentionPolicy DEFAULT_RETENTION_POLICY = new Builder().build();

        private final int _keepLastSuccessful;
        private final int _keepLastFailed;

        private final int _keepLastTotal;

        private final int _keepMaxDaysSuccessful;
        private final int _keepMaxDaysFailed;

        private final int _keepMaxDays;

        private RetentionPolicy(int keepLastSuccessful, int keepLastFailed, int keepLastTotal, int keepMaxDaysSuccessful,
                int keepMaxDaysFailed, int keepMaxDays) {
            _keepLastSuccessful = keepLastSuccessful;
            _keepLastFailed = keepLastFailed;
            _keepLastTotal = keepLastTotal;
            _keepMaxDaysSuccessful = keepMaxDaysSuccessful;
            _keepMaxDaysFailed = keepMaxDaysFailed;
            _keepMaxDays = keepMaxDays;
        }

        public static RetentionPolicy keepMaxDays(int keepMaxDays) {
            return new Builder().keepMaxDays(keepMaxDays).build();
        }

        public static class Builder {
            private int _keepLastSuccessful;
            private int _keepLastFailed;

            private int _keepLastTotal;

            private int _keepMaxDaysSuccessful;
            private int _keepMaxDaysFailed;

            private int _keepMaxDays = 365; // Default one year

            public Builder keepLastSuccessful(int keepLastSuccessful) {
                _keepLastSuccessful = keepLastSuccessful;
                return this;
            }

            public Builder keepLastFailed(int keepLastFailed) {
                _keepLastFailed = keepLastFailed;
                return this;
            }

            public Builder keepLastTotal(int keepLastTotal) {
                _keepLastTotal = keepLastTotal;
                return this;
            }

            public Builder keepMaxDaysSuccessful(int keepMaxDaysSuccessful) {
                _keepMaxDaysSuccessful = keepMaxDaysSuccessful;
                return this;
            }

            public Builder keepMaxDaysFailed(int keepMaxDaysFailed) {
                _keepLastFailed = keepMaxDaysFailed;
                return this;
            }

            public Builder keepMaxDays(int keepMaxDays) {
                _keepMaxDays = keepMaxDays;
                return this;
            }

            public RetentionPolicy build() {
                return new RetentionPolicy(_keepLastSuccessful, _keepLastFailed, _keepLastTotal,
                        _keepMaxDaysSuccessful, _keepMaxDaysFailed, _keepMaxDays);
            }
        }
    }

}
