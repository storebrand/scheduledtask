/*
 * Copyright 2022 Storebrand ASA
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.storebrand.scheduledtask;

import static java.util.stream.Collectors.toList;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import com.storebrand.scheduledtask.ScheduledTaskRegistry.LogEntry;
import com.storebrand.scheduledtask.ScheduledTaskRegistry.Schedule;
import com.storebrand.scheduledtask.ScheduledTaskRegistry.ScheduleRunContext;
import com.storebrand.scheduledtask.ScheduledTaskRegistry.ScheduleRunnable;
import com.storebrand.scheduledtask.ScheduledTaskRegistry.ScheduleStatus;
import com.storebrand.scheduledtask.ScheduledTaskRegistry.State;
import com.storebrand.scheduledtask.SpringCronUtils.CronExpression;
import com.storebrand.scheduledtask.db.ScheduledTaskRepository;
import com.storebrand.scheduledtask.db.ScheduledTaskRepository.ScheduledRunDto;

/**
 * The schedule class that is responsible for handling when it should run, sleep, pause and shutting down.
 *
 * @author Dag Bertelsen - dabe@skagenfondene.no - dabe@dagbertelsen.com - 2021.02
 * @author Kristian Hiim
 */
class ScheduledTaskRunner implements ScheduledTask {
    private static final Logger log = LoggerFactory.getLogger(ScheduledTaskRunner.class);
    private static final long SLEEP_TIME_MASTER_MAX_IN_MS = 2 * 60 * 1000; // 2 minutes
    private static final int SLEEP_TIME_SLAVE_MASTER_LOCK_CHECK_MS = 15 * 60 * 1_000; // 15 minutes
    private static final int ELIDED_LOG_LINES_ON_MASTER_SLEEP = 8;
    private static String DEFAULT_LOGGER_NAME = ScheduledTask.class.getName();

    private final ScheduledTaskConfig _config;
    private final ScheduledTaskRegistry _scheduledTaskRegistry;
    private final ScheduledTaskRepository _scheduledTaskRepository;
    private final Clock _clock;
    private final boolean _testMode;
    private Thread _runner;
    private final ScheduleRunnable _runnable;
    private final CronExpression _defaultCronExpression;
    private final Object _syncObject = new Object();
    private volatile CronExpression _overrideExpression;
    private volatile Instant _nextRun;
    private volatile Instant _currentRunStarted;
    private volatile Instant _lastRunCompleted;
    /* .. state vars .. */
    private volatile boolean _active = true;
    private volatile boolean _runFlag = true;
    private volatile boolean _isRunning = false;
    private volatile boolean _runOnce = false;

    ScheduledTaskRunner(ScheduledTaskConfig config, ScheduleRunnable runnable,
            ScheduledTaskRegistry scheduledTaskRegistry, ScheduledTaskRepository scheduledTaskRepository, Clock clock) {
        _config = config;
        _runnable = runnable;
        _defaultCronExpression = CronExpression.parse(config.getCronExpression());
        _scheduledTaskRegistry = scheduledTaskRegistry;
        _scheduledTaskRepository = scheduledTaskRepository;
        _clock = clock;
        log.info("Starting Thread '" + config.getName()
                + "' with the cronExpression '" + config.getCronExpression() + "'.");
        _runner = new Thread(this::runner,
                "ScheduledTask thread '" + config.getName() + "'");

        // :: Determine if we are running in test mode
        // If the scheduled task registry is in a special test mode then we don't start the thread runner, but instead
        // enable a special mode where we only run tasks by calling runNow().
        _testMode = (_scheduledTaskRegistry instanceof ScheduledTaskRegistryImpl) &&
                ((ScheduledTaskRegistryImpl)_scheduledTaskRegistry).isTestMode();

        // ?: Are we in test mode?
        if (!_testMode) {
            // -> No, good - start the runner.
            _runner.start();
        }
    }

    @SuppressWarnings({ "checkstyle:IllegalCatch", "MethodLength", "PMD.AvoidBranchingStatementAsLastInLoop" })
        // We want to log everything
    void runner() {
        MDC.put("scheduledTask", getName());
        int elidedSleepLogLines = 0;
        NEXTRUN:
        while (_runFlag) {
            try {
                SLEEP_LOOP:
                while (_runFlag) {
                    // get the next run from the db:
                    Optional<Schedule> scheduleFromDb = _scheduledTaskRepository.getSchedule(getName());
                    if (scheduleFromDb.isEmpty()) {
                        throw new RuntimeException("Schedule with name '" + getName() + " was not found'");
                    }

                    _nextRun = scheduleFromDb.get().getNextRun();
                    _runOnce = scheduleFromDb.get().isRunOnce();
                    // We may have set an override expression, so retrieve this and store it for use when we
                    // should update the nextRun
                    _overrideExpression = scheduleFromDb.get().getOverriddenCronExpression()
                            .map(CronExpression::parse)
                            .orElse(null);

                    // ?: Are we the master node?
                    if (_scheduledTaskRegistry.hasMasterLock()) {
                        // -> Yes, we are master and should only sleep if we are still waiting for the nextRun and if we
                        // are not set to run once.
                        if (_runOnce) {
                            // We should run once, so we should not sleep but instead run the schedule now.
                            log.info("Thread for Task '" + getName()
                                    + "' with nodeName '" + Host.getLocalHostName() + "' "
                                    + " is master and set to run once (NOW) and then continue as set in "
                                    + "schedule '" + getActiveCronExpressionInternal().toString() + "'.");
                        }
                        // E-> Have we passed the next run timestamp?
                        else if (Instant.now(_clock).isBefore(_nextRun)) {
                            // -> No, we have not yet passed the next run, so we should sleep a bit
                            long millisToWait = ChronoUnit.MILLIS.between(Instant.now(_clock), _nextRun);
                            // ?: Check that we have a positive number of milliseconds to wait
                            if (millisToWait > 0) {
                                // We should only sleep max SLEEP_LOOP_MAX_SLEEP_AMOUNT_IN_MILLISECONDS in
                                // one sleep loop.
                                millisToWait = Math.min(millisToWait, SLEEP_TIME_MASTER_MAX_IN_MS);
                                String message = "Thread for Task '" + getName() + "', "
                                        + " master node '" + Host.getLocalHostName() + "' "
                                        + "is going to sleep for '" + millisToWait
                                        + "' ms and wait for the next schedule run '" + _nextRun + "'.";
                                // To avoid spamming the log too hard we only log to info every x sleep cycles.
                                if (elidedSleepLogLines++ >= ELIDED_LOG_LINES_ON_MASTER_SLEEP) {
                                    log.info(message + " NOTE: Elided this log line " + elidedSleepLogLines + " times.");
                                    elidedSleepLogLines = 0;
                                }
                                else {
                                    log.debug(message);
                                }

                                synchronized (_syncObject) {
                                    _syncObject.wait(millisToWait);
                                }
                            }
                            else {
                                log.info("Already passed next run, so we are going to run now, millis past nextRun: "
                                        + "[" + millisToWait + "]");
                            }
                        }
                    }
                    else {
                        // -> No, we are not master, so we should sleep for 15min or until we are master and
                        // are awoken again
                        synchronized (_syncObject) {
                            log.info("Thread for Task '" + getName() + "', "
                                    + " slave node '" + Host.getLocalHostName() + "' "
                                    + "is going to sleep for 15 min.");
                            _syncObject.wait(SLEEP_TIME_SLAVE_MASTER_LOCK_CHECK_MS);
                        }
                    }

                    // We have exited the sleep.
                    log.debug("Thread for Task '" + getName()
                            + "' with nodeName '" + Host.getLocalHostName() + "' "
                            + " exited the sleep.");
                    _isRunning = true;

                    // ?: Check again for runThreads - as this will happen in shutdown
                    if (!_runFlag) {
                        // -> Yes, we're stopping.
                        log.info("Thread for Task '" + getName()
                                + "' with nodeName '" + Host.getLocalHostName() + "' "
                                + " is exiting the schedule loop.");
                        _isRunning = false;
                        break NEXTRUN;
                    }

                    // ?: Are we master?
                    if (!_scheduledTaskRegistry.hasMasterLock()) {
                        // -> No, not master at the moment.

                        log.info("Thread for Task '" + getName()
                                + "' with nodeName '" + Host.getLocalHostName() + "' "
                                + " is not currently master so restarting sleep loop, next run is set"
                                + " to '" + _nextRun + "'");
                        _isRunning = false;
                        continue SLEEP_LOOP;
                    }

                    // Re-retrieve the schedule from the db. It may have changed during the sleep:
                    scheduleFromDb = _scheduledTaskRepository.getSchedule(getName());
                    if (!scheduleFromDb.isPresent()) {
                        throw new RuntimeException("Schedule with name '" + getName() + " was not found'");
                    }
                    // We may have set an override expression, so retrieve this and store it for use when we
                    // should update the nextRun
                    _overrideExpression = scheduleFromDb.get().getOverriddenCronExpression()
                            .map(CronExpression::parse)
                            .orElse(null);
                    _nextRun = scheduleFromDb.get().getNextRun();
                    _active = scheduleFromDb.get().isActive();
                    _runOnce = scheduleFromDb.get().isRunOnce();

                    // ?: Check if we should run now once regardless of when the schedule should actually run
                    if (_runOnce) {
                        // -> Yes, we should only run once and then continue on the normal schedule plans.
                        _scheduledTaskRepository.setRunOnce(getName(), false);
                        log.info("Thread for Task '" + getName()
                                + "' with nodeName '" + Host.getLocalHostName() + "' "
                                + " is set to run once (NOW) and then continue as set in "
                                + "schedule '" + getActiveCronExpressionInternal().toString() + "'.");
                        break SLEEP_LOOP;
                    }

                    // ?: Have we passed the nextRun instance
                    if (Instant.now(_clock).isBefore(_nextRun)) {
                        // -> No, we have not yet passed the nextRun (now is still before nextRun)
                        // so we should do another sleep cycle.
                        log.debug("Thread for Task '" + getName()
                                + "' with nodeName '" + Host.getLocalHostName() + "' "
                                + " has to sleep a bit longer "
                                + "since we have not passed the nextRun instance.");
                        _isRunning = false;
                        continue SLEEP_LOOP;
                    }

                    // ----- _runFlag is still true, we are master, and nextRun is after the now(),
                    // exit the SLEEP_LOOP And run the schedule
                    log.debug("Thread for Task '" + getName()
                            + "' with nodeName '" + Host.getLocalHostName() + "' "
                            + " is master and nextRun has passed now() exiting SLEEP_LOOP to do the schedule run ");
                    break SLEEP_LOOP;
                } // SLEEP_LOOP end

                // ?: Finally we have an option to pause the schedule, this works by only bypassing the run part and
                // doing a new sleep cycle to the next run.
                if (!ScheduledTaskRunner.this._active) {
                    // -> No, we have paused the schedule so we should skip this run and do another sleep cycle.
                    Instant nextRun = nextScheduledRun(getActiveCronExpressionInternal(), Instant.now(_clock));
                    log.info("Thread for Task '" + getName()
                            + "' is currently deactivated so "
                            + "we are skipping this run and setting next run to '" + nextRun + "'");
                    _scheduledTaskRepository.updateNextRun(getName(), getOverrideExpressionAsString(), nextRun);
                    _isRunning = false;
                    _lastRunCompleted = Instant.now(_clock);
                    continue NEXTRUN;
                }

                // ----- We have not updated the cronExpression or the cronExpression is updated to be before the
                // previous set one. In any regards we have passed the moment where the schedule should run and we
                // have verified that we should not stop/skip the schedule
                runTask();
                // If the schedule goes often then there is no use in logging the "im still sleeping" log lines.
                elidedSleepLogLines = 0;

                // :: Execute retention policy
                // TODO: Only execute at given intervals?
                _scheduledTaskRepository.executeRetentionPolicy(getName(), _config.getRetentionPolicy());
            }
            catch (Throwable t) {
                // Since we got an exception, we are not currently running the schedule:
                _isRunning = false;

                // ?: Check again for runThreads - as this will happen in shutdown
                if (!_runFlag) {
                    // -> Yes, we're stopping.
                    log.info("Thread for Task '" + getName() + " got '" + t.getClass()
                            .getSimpleName()
                            + "' from the run-loop, but runThread-flag was false, so we're evidently exiting.");
                    break;
                }

                log.warn("Thread for Task '" + getName() + " got a '" + t.getClass()
                        .getSimpleName()
                        + "' from the run-loop. Chilling a little, then trying again.", t);
                try {
                    Thread.sleep(5000);
                }
                catch (InterruptedException e) {
                    log.info("Thread for Task '" + getName() + " got interrupted while "
                            + "chill-waiting after an Exception when trying to do the schedule run"
                            + ". Ignoring by looping, thus checking runThread-flag.");
                }
            }
        }

        // Exiting loop, so clear the runner thread and log that we are now shutting down.
        _runner = null;
        log.info("Thread for Task '" + getName() + "' asked to exit, shutting down!");
    }

    /**
     * Actually runs the task, and logs if it fails.
     */
    private void runTask() {
        log.info("Thread for Task '" + getName()
                + "' is beginning to do the run according "
                + "to the set schedule '" + getActiveCronExpressionInternal().toString() + "'.");
        _currentRunStarted = Instant.now(_clock);
        long id = _scheduledTaskRepository.addScheduleRun(getName(), Host.getLocalHostName(),
                _currentRunStarted, "Schedule run starting.");
        ScheduleRunContext ctx = new ScheduledTaskRunnerContext(id, ScheduledTaskRunner.this,
                Host.getLocalHostName(), _scheduledTaskRepository, _clock, _currentRunStarted);


        // ?: Is this schedule manually triggered? Ie set to run once.
        if (ScheduledTaskRunner.this._runOnce) {
            // -> Yes, this where set to run once and is manually triggered. so add a log line.
            // note, this is named runNow in the gui but runOnce in the db so we use the term used
            // in gui for the logging.
            ctx.log("Manually started");
        }

        // :: Try to run the code that the user wants, log if it fails.
        try {
            ScheduleStatus status = _runnable.run(ctx);
            if (!(status instanceof ScheduleStatusValidResponse)) {
                throw new IllegalArgumentException("The ScheduleRunContext returned a invalid ScheduleStatus,"
                        + " make sure you are using done(), failed() or dispatched()");
            }
        }
        catch (Throwable t) {
            // Oh no, the schedule we set to run failed, we should log this and continue
            // on the normal schedule loop
            String message = "Schedule '" + getName() + " run failed due to an error.' ";
            log.info(message);
            ctx.failed(message, t);
        }
        _lastRunCompleted = Instant.now(_clock);

        _isRunning = false;
        Instant nextRun = nextScheduledRun(getActiveCronExpressionInternal(), Instant.now(_clock));
        log.info("Thread for Task '" + getName() + "' "
                + " runId '" + ctx.getRunId() + "' "
                + "used '" + Duration.between(_currentRunStarted, _lastRunCompleted).toMillis() + "' "
                + "ms to run. Setting next run to '" + nextRun + "', "
                + "using CronExpression '" + getActiveCronExpressionInternal() + "'");

        _scheduledTaskRepository.updateNextRun(getName(), getOverrideExpressionAsString(), nextRun);
    }

    /**
     * The cron expression currently set on the scheduler. Usually we only use the {@link #_defaultCronExpression} but
     * we can override this in the monitor so we should check if the {@link #_overrideExpression} have been set.
     */
    CronExpression getActiveCronExpressionInternal() {
        CronExpression overrideExpression = _overrideExpression;
        // :? Is the cron expression set
        if (overrideExpression == null) {
            // -> No, use the default cronExpression.
            return _defaultCronExpression;
        }
        return overrideExpression;
    }

    /**
     * Returns the current override expression, or null if there is no override expression.
     */
    String getOverrideExpressionAsString() {
        CronExpression overrideExpression = _overrideExpression;
        return overrideExpression != null
                ? overrideExpression.toString()
                : null;
    }

    /**
     * The cron expression currently set on the scheduler. Usually we only use the {@link #_defaultCronExpression} but
     * we can override this in the monitor so we should check if the {@link #_overrideExpression} have been set.
     */
    @Override
    public String getActiveCronExpression() {
        return getActiveCronExpressionInternal().toString();
    }

    /**
     * Return the default cron expression set to the schedule when it is created.
     */
    @Override
    public String getDefaultCronExpression() {
        return _defaultCronExpression.toString();
    }

    /**
     * Set a new cronExpression to be used by the schedule. If this is set to null it will fall back to use the {@link
     * #_defaultCronExpression} again.
     */
    @Override
    public void setOverrideExpression(String newCronExpression) {
        CronExpression parsedNewCronExpression = newCronExpression != null
                ? CronExpression.parse(newCronExpression) : null;
        _overrideExpression = parsedNewCronExpression;
        _scheduledTaskRepository.updateNextRun(getName(), newCronExpression,
                nextScheduledRun(parsedNewCronExpression, Instant.now(_clock)));
        // Updated cron, wake thread. The next run may be earlier than the previous set sleep cycle.
        notifyThread();
    }

    /**
     * Check if the schedule task thread is active. This should in theory always be true, but if the thread has been
     * stopped by some external means it will return false.
     */
    @Override
    public boolean isThreadAlive() {
        return _runner != null && _runner.isAlive();
    }

    /**
     * Checks if this scheduler is running or if it is currently stopped. paused.
     */
    @Override
    public boolean isActive() {
        return _active;
    }

    /**
     * Set the schedule to be active when set to true or paused if set to false.
     */
    void setActive(boolean isActive) {
        _scheduledTaskRepository.setActive(getName(), isActive);
        _active = isActive;
    }

    @Override
    public String getName() {
        return _config.getName();
    }

    @Override
    public Criticality getCriticality() {
        return _config.getCriticality();
    }

    @Override
    public Recovery getRecovery() {
        return _config.getRecovery();
    }

    @Override
    public RetentionPolicy getRetentionPolicy() {
        return _config.getRetentionPolicy();
    }

    @Override
    public void start() {
        setActive(true);
    }

    @Override
    public void stop() {
        setActive(false);
    }

    @Override
    public Optional<ScheduleRunContext> getLastScheduleRun() {
        return _scheduledTaskRepository.getLastRunForSchedule(getName())
                .map(lastRun -> new ScheduledTaskRunnerContext(lastRun, this, _scheduledTaskRepository, _clock));
    }

    @Override
    public List<ScheduleRunContext> getAllScheduleRunsBetween(LocalDateTime from, LocalDateTime to) {
        return _scheduledTaskRepository.getScheduleRunsBetween(getName(), from, to).stream()
                .map(scheduledRunDto -> new ScheduledTaskRunnerContext(scheduledRunDto, this,
                        _scheduledTaskRepository, _clock))
                .collect(toList());

    }

    @Override
    public Map<Long, List<LogEntry>> getLogEntriesByRunId(LocalDateTime from, LocalDateTime to) {
        return _scheduledTaskRepository.getLogEntriesByRunId(getName(), from, to);
    }

    /**
     * Shows if the current schedule is currently running. This will be set to <i>true</i> when a scheduled run starts
     * and to <i>false</i> when it stops.
     * <p>
     * This together with {@link #isOverdue()} shows if the current schedule is alive and working as expected.
     */
    @Override
    public boolean isRunning() {
        return _isRunning;
    }

    /**
     * Checks if this run has passed expected run time.
     */
    @Override
    public boolean hasPassedExpectedRunTime(Instant lastRunStarted) {
        // ?: Have we started at all?
        if (lastRunStarted == null) {
            // -> No, so we have not passed expected run time
            return false;
        }
        // E-> We have started, so we should check if we have passed expected run time
        return Instant.now(_clock).isAfter(
                nextScheduledRun(getActiveCronExpressionInternal(), lastRunStarted)
                // Two minute grace time
                .plus(2, ChronoUnit.MINUTES));
    }

    /**
     * Checks if the current run is taking longer time than the {@link #_config#getMaxExpectedMinutesToRun()}.
     */
    @Override
    public boolean isOverdue() {
        Optional<Long> runTime = runTimeInMinutes();
        // ?: Did we get any runtime?
        if (runTime.isEmpty()) {
            // -> No, so it is not overdue
            return false;
        }
        return runTime.get() >= getMaxExpectedMinutesToRun();
    }

    @Override
    public Optional<Long> runTimeInMinutes() {
        // Is this schedule started at all?
        if (_currentRunStarted == null) {
            // ->NO, so it is not overdue
            return Optional.empty();
        }

        // ?: Is _lastRunCompleted set and is currentRunStarted before _lastRunCompleted.
        // If so, it means we are not currently running
        if (_lastRunCompleted != null && _currentRunStarted.isBefore(_lastRunCompleted)) {
            // -> Yes, we are not currently running, so we are not overdue
            return Optional.empty();
        }

        // ?: Are we currently running, we are only overdue if we are running
        if (!isRunning()) {
            // -> No, we are not running so we are not overdue at the moment
            return Optional.empty();
        }
        return Optional.of(ChronoUnit.MINUTES.between(_currentRunStarted, Instant.now(_clock)));

    }

    /**
     * Retrieve the max amount of minutes this schedule is expected to run.
     */
    @Override
    public int getMaxExpectedMinutesToRun() {
        return _config.getMaxExpectedMinutesToRun();
    }

    @Override
    public ScheduleRunContext getInstance(long runId) {
        Optional<ScheduledRunDto> scheduleRun = _scheduledTaskRepository.getScheduleRun(runId);

        // ?: Did we find the instance?
        return scheduleRun
                // -> yes we found the schedule run
                .map(scheduledRunDto ->
                        new ScheduledTaskRunnerContext(scheduledRunDto, this, _scheduledTaskRepository, _clock))
                // -> No, just return null
                .orElse(null);

    }

    /**
     * Retrieve the {@link #_lastRunCompleted}
     */
    @Override
    public Instant getLastRunCompleted() {
        return _lastRunCompleted;
    }

    /**
     * Retrieve the {@link #_currentRunStarted}
     */
    @Override
    public Instant getLastRunStarted() {
        return _currentRunStarted;
    }

    /**
     * Retrieve the {@link #_nextRun}
     */
    @Override
    public Instant getNextRun() {
        return _nextRun;
    }

    /**
     * Causes this thread to wake up from sleep.
     */
    void notifyThread() {
        synchronized (_syncObject) {
            _syncObject.notifyAll();
        }
    }

    /**
     * Make this tread completely stop
     */
    void killSchedule() {
        _runFlag = false;
        // Then wake the tread so it is aware that we have updated the runFlag:
        notifyThread();
    }

    /**
     * The time the scheduler is expected to run next.
     */
    Instant nextScheduledRun(CronExpression cronExpression, Instant instant) {
        // ?: Is the cronExpression null, if it is we should use the default one instead
        CronExpression cronExpressionToUse = cronExpression != null ? cronExpression : _defaultCronExpression;
        LocalDateTime dateTime = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
        LocalDateTime nextRun = cronExpressionToUse.next(dateTime);
        if (nextRun != null) {
            return nextRun.atZone(ZoneId.systemDefault()).toInstant();
        }

        // E-> Not able to figure out the next run
        return null;
    }

    /**
     * Manual trigger the schedule to run now.
     */
    @Override
    public void runNow() {
        // ?: Are we in special test mode?
        if (_testMode) {
            // -> Yes, then we just run the task, and return.
            log.info("Running task [" + getName() + "] explicitly in test mode"
                    + " - this should never happen unless we are inside a test.");
            runTask();
            return;
        }

        // Write to db we should run this schedule now regardless of the cronExpression
        _scheduledTaskRepository.setRunOnce(getName(), true);
        // Then wake the tread so it is aware that we should run now once
        synchronized (_syncObject) {
            _syncObject.notifyAll();
        }
    }

    // ===== Internal helper classes ===================================================================================

    /**
     * Implementation of the {@link ScheduleRunContext}, it is indirectly used when a schedule is running and
     * this run is appending {@link #log(String)}, {@link #dispatched(String)}, {@link #done(String)}
     * or {@link #failed(String)}
     */
    private static class ScheduledTaskRunnerContext implements ScheduleRunContext {
        private final ScheduledTaskRunner _storebrandSchedule;
        private final ScheduledTaskRepository _scheduledTaskRepository;
        private final long _runId;
        private final Clock _clock;
        private final String _hostname;
        private final ScheduledRunDto _scheduledRunDto;

        /**
         * Create a new {@link ScheduleRunContext}, IE a brand new run.
         */
        private ScheduledTaskRunnerContext(long runId, ScheduledTaskRunner storebrandSchedule, String hostname,
                ScheduledTaskRepository scheduledTaskRepository, Clock clock, Instant runStart) {
            _runId = runId;
            _storebrandSchedule = storebrandSchedule;
            _clock = clock;
            _scheduledTaskRepository = scheduledTaskRepository;
            _hostname = hostname;
            _scheduledRunDto = ScheduledRunDto.newWithStateStarted(_runId, storebrandSchedule.getName(), hostname, runStart);
        }

        /**
         * Reload a previous (probably ongoing) run from the database. This is both used for GUI elements (look up last,
         * and between date ranges), and for the cases where the async "dispatch" concept has been used, where a task
         * has been e.g. send on a Mats process, and then e.g. in a later stage, or a terminator, one wants to log more,
         * set new status, or finish the task.
         */
        private ScheduledTaskRunnerContext(ScheduledRunDto scheduledRunDto, ScheduledTaskRunner storebrandSchedule,
                ScheduledTaskRepository scheduledTaskRepository, Clock clock) {
            _runId = scheduledRunDto.getRunId();
            _storebrandSchedule = storebrandSchedule;
            _clock = clock;
            _scheduledTaskRepository = scheduledTaskRepository;
            _hostname = scheduledRunDto.getHostname();
            _scheduledRunDto = scheduledRunDto;
        }

        @Override
        public long getRunId() {
            return _runId;
        }

        @Override
        public String getScheduledName() {
            return _storebrandSchedule.getName();
        }

        @Override
        public String getHostname() {
            return _hostname;
        }

        @Override
        public ScheduledTask getSchedule() {
            return _storebrandSchedule;
        }

        @Override
        public Instant getPreviousRun() {
            return _scheduledTaskRepository.getLastRunForSchedule(_storebrandSchedule.getName())
                    .map(ScheduledRunDto::getRunStart)
                    .map(localDateTime -> localDateTime.atZone(ZoneId.systemDefault()).toInstant())
                    .orElse(null);
        }

        @Override
        public State getStatus() {
            return _scheduledRunDto.getStatus();
        }

        @Override
        public String getStatusMsg() {
            return _scheduledRunDto.getStatusMsg();
        }

        @Override
        public String getStatusStackTrace() {
            return _scheduledRunDto.getStatusStackTrace();
        }

        @Override
        public LocalDateTime getRunStarted() {
            return _scheduledRunDto.getRunStart();
        }

        @Override
        public LocalDateTime getStatusTime() {
            return _scheduledRunDto.getStatusTime();
        }

        @Override
        public List<LogEntry> getLogEntries() {
            return _scheduledTaskRepository.getLogEntries(_runId);
        }

        @Override
        public void log(String msg) {
            String prevTaskName = MDC.get("scheduledTask");
            MDC.put("scheduledTask", _storebrandSchedule.getName());
            Logger stackContextLogger = LoggerFactory.getLogger(getInvocationPoint());
            try {
                stackContextLogger.info(msg);
                _scheduledTaskRepository.addLogEntry(_runId, LocalDateTime.now(_clock), msg);
            }
            finally {
                if (prevTaskName != null) {
                    MDC.put("scheduledTask", prevTaskName);
                }
                else {
                    MDC.remove("scheduledTask");
                }
            }
        }

        @Override
        public void log(String msg, Throwable throwable) {
            String prevTaskName = MDC.get("scheduledTask");
            MDC.put("scheduledTask", _storebrandSchedule.getName());
            Logger stackContextLogger = LoggerFactory.getLogger(getInvocationPoint());
            try {
                stackContextLogger.info(msg, throwable);
                _scheduledTaskRepository.addLogEntry(_runId, LocalDateTime.now(_clock), msg,
                        throwableToStackTraceString(throwable));
            }
            finally {
                if (prevTaskName != null) {
                    MDC.put("scheduledTask", prevTaskName);
                }
                else {
                    MDC.remove("scheduledTask");
                }
            }
        }

        @Override
        public ScheduleStatus done(String msg) {
            _scheduledRunDto.setStatus(State.DONE, Instant.now(_clock), msg);
            _scheduledTaskRepository.setStatus(_scheduledRunDto);
            log("[" + State.DONE + "] " + msg);
            return new ScheduleStatusValidResponse();
        }

        @Override
        public ScheduleStatus failed(String msg) {
            _scheduledRunDto.setStatus(State.FAILED, Instant.now(_clock), msg);
            _scheduledTaskRepository.setStatus(_scheduledRunDto);
            log("[" + State.FAILED + "] " + msg);
            return new ScheduleStatusValidResponse();
        }

        @Override
        public ScheduleStatus failed(String msg, Throwable throwable) {
            _scheduledRunDto.setStatus(State.FAILED, Instant.now(_clock), msg, throwableToStackTraceString(throwable));
            _scheduledTaskRepository.setStatus(_scheduledRunDto);
            log("[" + State.FAILED + "] " + msg, throwable);
            return new ScheduleStatusValidResponse();
        }

        @Override
        public ScheduleStatus dispatched(String msg) {
            _scheduledRunDto.setStatus(State.DISPATCHED, Instant.now(_clock), msg);
            _scheduledTaskRepository.setStatus(_scheduledRunDto);
            log("[" + State.DISPATCHED + "] " + msg);
            return new ScheduleStatusValidResponse();
        }

        @Override
        public ScheduleStatus noop(String msg) {
            _scheduledRunDto.setStatus(State.NOOP, Instant.now(_clock), msg);
            _scheduledTaskRepository.setStatus(_scheduledRunDto);
            log("[" + State.NOOP + "] " + msg);
            return new ScheduleStatusValidResponse();
        }
    }

    /**
     * Copied from Mats3, inspired from <a href="https://stackoverflow.com/a/11306854">Stackoverflow - Denys Séguret</a>.
     *
     * @return a String showing where this code was invoked from, like "Test.java.123;com.example.Test;methodName()"
     */
    private static String getInvocationPoint() {
        StackTraceElement[] stElements = Thread.currentThread().getStackTrace();
        for (int i = 1; i < stElements.length; i++) {
            StackTraceElement ste = stElements[i];
            if (ste.getClassName().startsWith("com.storebrand.scheduledtask.")) {
                continue;
            }
            // ?: If we've come to the bottom of the stack, we did not find any non-ScheduledTask stack frames.
            if (ste.getClassName().equals("java.lang.Thread")) {
                // -> Yes, only found "java.lang.Thread", which means there was no non-ScheduledTask stack frames.
                return DEFAULT_LOGGER_NAME;
            }
            // E-> We have an invocation point, return the class name
            return ste.getClassName();
        }
        // E-> Evidently no stackframes!?
        return DEFAULT_LOGGER_NAME;
    }

    /**
     * NO-OP class only used to make sure the users are calling the {@link ScheduleRunContext#done(String)}, {@link
     * ScheduleRunContext#failed(String)} or {@link ScheduleRunContext#dispatched(String)}
     */
    static class ScheduleStatusValidResponse implements ScheduleStatus {

    }

    // ===== Helper method =============================================================================================

    /**
     * Converts a throwable to string.
     */
    static String throwableToStackTraceString(Throwable throwable) {
        if (throwable == null) {
            return null;
        }

        // Convert the stackTrace to string;
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        throwable.printStackTrace(pw);
        return sw.toString();
    }
}
