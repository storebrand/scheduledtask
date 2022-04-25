package com.storebrand.scheduledtask.healthcheck;

import java.util.Map;
import java.util.Optional;

import com.storebrand.healthcheck.HealthCheckRegistry;
import com.storebrand.scheduledtask.ScheduledTask;
import com.storebrand.scheduledtask.ScheduledTask.Criticality;
import com.storebrand.scheduledtask.ScheduledTask.Recovery;
import com.storebrand.scheduledtask.ScheduledTaskBuilder;
import com.storebrand.scheduledtask.ScheduledTaskService;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Wrapper for {@link ScheduledTaskService} that hooks Storebrand HealthChecks up, and ensures that all scheduled tasks
 * that are added are properly specified in the health check. This is done by calling
 * {@link ScheduledTaskHealthCheck#reSpecifyHealthCheck()} after each new scheduled task has been added.
 *
 * @author Kristian Hiim
 */
public class ScheduledTaskServiceHealthCheckWrapper implements ScheduledTaskService {

    private final ScheduledTaskService _scheduledTaskService;
    private final ScheduledTaskHealthCheck _scheduledTaskHealthCheck;

    @SuppressFBWarnings(value = "EI_EXPOSE_REP2", justification = "This is standard dependency injection.")
    public ScheduledTaskServiceHealthCheckWrapper(ScheduledTaskService scheduledTaskService,
            ScheduledTaskHealthCheck scheduledTaskHealthCheck,
            HealthCheckRegistry healthCheckRegistry) {
        _scheduledTaskService = scheduledTaskService;
        _scheduledTaskHealthCheck = scheduledTaskHealthCheck;
        _scheduledTaskHealthCheck.initialize(this, healthCheckRegistry);
    }

    @Override
    public ScheduledTaskBuilder buildScheduledTask(String name, String cronExpression, ScheduleRunnable runnable) {
        return new ScheduledTaskBuilderHealthCheckWrapper(
                _scheduledTaskService.buildScheduledTask(name, cronExpression, runnable));
    }

    @Override
    public ScheduledTask getScheduledTask(String name) {
        return _scheduledTaskService.getScheduledTask(name);
    }

    @Override
    public Map<String, ScheduledTask> getScheduledTasks() {
        return _scheduledTaskService.getScheduledTasks();
    }

    @Override
    public Map<String, Schedule> getSchedulesFromRepository() {
        return _scheduledTaskService.getSchedulesFromRepository();
    }

    @Override
    public Optional<MasterLock> getMasterLock() {
        return _scheduledTaskService.getMasterLock();
    }

    @Override
    public boolean hasMasterLock() {
        return _scheduledTaskService.hasMasterLock();
    }

    @Override
    public void close() {
        _scheduledTaskService.close();
    }

    @SuppressFBWarnings(value = "RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT",
            justification = "This is intentional, as we want to return the wrapper, not the wrapped initializer.")
    class ScheduledTaskBuilderHealthCheckWrapper implements ScheduledTaskBuilder {

        private final ScheduledTaskBuilder _initializer;

        ScheduledTaskBuilderHealthCheckWrapper(ScheduledTaskBuilder initializer) {
            _initializer = initializer;
        }

        @Override
        public ScheduledTaskBuilder maxExpectedMinutesToRun(int minutes) {
            _initializer.maxExpectedMinutesToRun(minutes);
            return this;
        }

        @Override
        public ScheduledTaskBuilder criticality(Criticality criticality) {
            _initializer.criticality(criticality);
            return this;
        }

        @Override
        public ScheduledTaskBuilder recovery(Recovery recovery) {
            _initializer.recovery(recovery);
            return this;
        }

        @Override
        public ScheduledTaskBuilder deleteRunsAfterDays(int days) {
            _initializer.deleteRunsAfterDays(days);
            return this;
        }

        @Override
        public ScheduledTaskBuilder deleteSuccessfulRunsAfterDays(int days) {
            _initializer.deleteSuccessfulRunsAfterDays(days);
            return this;
        }

        @Override
        public ScheduledTaskBuilder deleteFailedRunsAfterDays(int days) {
            _initializer.deleteFailedRunsAfterDays(days);
            return this;
        }

        @Override
        public ScheduledTaskBuilder keepMaxRuns(int maxRuns) {
            _initializer.keepMaxRuns(maxRuns);
            return this;
        }

        @Override
        public ScheduledTaskBuilder keepMaxSuccessfulRuns(int maxSuccessfulRuns) {
            _initializer.keepMaxSuccessfulRuns(maxSuccessfulRuns);
            return this;
        }

        @Override
        public ScheduledTaskBuilder keepMaxFailedRuns(int maxFailedRuns) {
            _initializer.keepMaxFailedRuns(maxFailedRuns);
            return this;
        }

        @Override
        public ScheduledTask start() {
            ScheduledTask scheduledTask = _initializer.start();
            _scheduledTaskHealthCheck.reSpecifyHealthCheck();
            return scheduledTask;
        }
    }
}
