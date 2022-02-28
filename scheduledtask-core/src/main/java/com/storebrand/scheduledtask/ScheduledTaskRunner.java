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
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.storebrand.scheduledtask.ScheduledTaskService.LogEntry;
import com.storebrand.scheduledtask.ScheduledTaskService.Schedule;
import com.storebrand.scheduledtask.ScheduledTaskService.ScheduleRunContext;
import com.storebrand.scheduledtask.ScheduledTaskService.ScheduleRunnable;
import com.storebrand.scheduledtask.ScheduledTaskService.ScheduleStatus;
import com.storebrand.scheduledtask.ScheduledTaskService.State;
import com.storebrand.scheduledtask.ScheduledTaskServiceImpl.MasterLockKeeper;
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
    private static final long SLEEP_LOOP_MAX_SLEEP_AMOUNT_IN_MILLISECONDS = 2 * 60 * 1000; // 2 minutes

    private final ScheduledTaskConfig _config;
    private Thread _runner;
    private final ScheduleRunnable _runnable;
    private final CronExpression _defaultCronExpression;
    private volatile CronExpression _overrideExpression;
    private volatile Instant _nextRun;
    private volatile Instant _currentRunStarted;
    private volatile Instant _lastRunCompleted;
    /* .. state vars .. */
    private volatile boolean _active = true;
    private volatile boolean _runFlag = true;
    private volatile boolean _isRunning = false;
    private volatile boolean _runOnce = false;
    private final Object _syncObject = new Object();
    private final MasterLockKeeper _masterLockKeeper;
    private final ScheduledTaskRepository _scheduledTaskRepository;
    private final Clock _clock;

    ScheduledTaskRunner(ScheduledTaskConfig config, ScheduleRunnable runnable,
            MasterLockKeeper masterLockKeeper, ScheduledTaskRepository scheduledTaskRepository, Clock clock) {
        _config = config;
        _runnable = runnable;
        _defaultCronExpression = CronExpression.parse(config.getCronExpression());
        _masterLockKeeper = masterLockKeeper;
        _scheduledTaskRepository = scheduledTaskRepository;
        _clock = clock;
        log.info("Starting Thread '" + config.getName()
                + "' with the cronExpression '" + config.getCronExpression() + "'.");
        _runner = new Thread(this::runner,
                "ScheduledTask thread '" + config.getName() + "'");
        _runner.start();
    }

    @SuppressWarnings({ "checkstyle:IllegalCatch", "MethodLength", "PMD.AvoidBranchingStatementAsLastInLoop" })
        // We want to log everything
    void runner() {
        NEXTRUN:
        while (_runFlag) {
            try {
                SLEEP_LOOP:
                while (_runFlag) {
                    // get the next run from the db:
                    Optional<Schedule> scheduleFromDb = _scheduledTaskRepository.getSchedule(getScheduleName());
                    if (!scheduleFromDb.isPresent()) {
                        throw new RuntimeException("Schedule with name '" + getScheduleName() + " was not found'");
                    }

                    _nextRun = scheduleFromDb.get().getNextRun();
                    // We may have set an override expression, so retrieve this and store it for use when we
                    // should update the nextRun
                    _overrideExpression = scheduleFromDb.get().getOverriddenCronExpression()
                            .map(CronExpression::parse)
                            .orElse(null);

                    // ?: Are we the master node
                    if (_masterLockKeeper.isMaster()) {
                        // -> Yes, we are master and should only sleep if we are still waiting for the nextRun.
                        if (Instant.now(_clock).isBefore(_nextRun)) {
                            // -> No, we have not yet passed the next run so we should sleep a bit
                            long millisToWait = ChronoUnit.MILLIS.between(Instant.now(_clock), _nextRun);
                            // We should only sleep only max SLEEP_LOOP_MAX_SLEEP_AMOUNT_IN_MILLISECONDS in
                            // one sleeop loop.
                            millisToWait = Math.min(millisToWait, SLEEP_LOOP_MAX_SLEEP_AMOUNT_IN_MILLISECONDS);
                            synchronized (_syncObject) {
                                log.debug("Thread '" + ScheduledTaskRunner.this.getScheduleName() + "', "
                                        + " master node '" + Host.getLocalHostName() + "' "
                                        + "is going to sleep for '" + millisToWait
                                        + "' ms and wait for the next schedule run.");
                                _syncObject.wait(millisToWait);
                            }
                        }
                    }
                    else {
                        // -> No, we are not master, so we should sleep for 15min or until we are master and
                        // are awoken again
                        synchronized (_syncObject) {
                            log.info("Thread '" + ScheduledTaskRunner.this.getScheduleName() + "', "
                                    + " slave node '" + Host.getLocalHostName() + "' "
                                    + "is going to sleep for 15 min.");
                            _syncObject.wait(15 * 60 * 1_000);
                        }
                    }

                    // We have exited the sleep.
                    log.debug("Thread '" + ScheduledTaskRunner.this.getScheduleName()
                            + "' with nodeName '" + Host.getLocalHostName() + "' "
                            + " exited the sleep.");
                    _isRunning = true;

                    // ?: Check again for runThreads - as this will happen in shutdown
                    if (!_runFlag) {
                        // -> Yes, we're stopping.
                        log.info("Thread '" + ScheduledTaskRunner.this.getScheduleName()
                                + "' with nodeName '" + Host.getLocalHostName() + "' "
                                + " is exiting the schedule loop.");
                        _isRunning = false;
                        break NEXTRUN;
                    }

                    // ?: Are we master
                    if (!_masterLockKeeper.isMaster()) {
                        Instant nextRun = nextScheduledRun(getActiveCronExpressionInternal(), Instant.now(_clock));

                        log.info("Thread '" + ScheduledTaskRunner.this.getScheduleName()
                                + "' with nodeName '" + Host.getLocalHostName() + "' "
                                + " is not currently master so restarting sleep loop, next run is set"
                                + " to '" + nextRun + "'");
                        _isRunning = false;
                        continue SLEEP_LOOP;
                    }

                    // Re-retrieve the schedule from the db. It may have changed during the sleep:
                    scheduleFromDb = _scheduledTaskRepository.getSchedule(getScheduleName());
                    if (!scheduleFromDb.isPresent()) {
                        throw new RuntimeException("Schedule with name '" + getScheduleName() + " was not found'");
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
                        _scheduledTaskRepository.setRunOnce(getScheduleName(), false);
                        log.info("Thread '" + ScheduledTaskRunner.this.getScheduleName()
                                + "' with nodeName '" + Host.getLocalHostName() + "' "
                                + " is set to run once (NOW) and then continue as set in "
                                + "schedule '" + getActiveCronExpressionInternal().toString() + "'.");
                        break SLEEP_LOOP;
                    }

                    // ?: Have we passed the nextRun instance
                    if (Instant.now(_clock).isBefore(_nextRun)) {
                        // -> No, we have not yet passed the nextRun (now is still before nextRun)
                        // so we should do another sleep cycle.
                        log.debug("Thread '" + ScheduledTaskRunner.this.getScheduleName()
                                + "' with nodeName '" + Host.getLocalHostName() + "' "
                                + " has to sleep a bit longer "
                                + "since we have not passed the nextRun instance.");
                        _isRunning = false;
                        continue SLEEP_LOOP;
                    }

                    // ----- _runFlag is still true, we are master, and nextRun is after the now(),
                    // exit the SLEEP_LOOP And run the schedule
                    log.debug("Thread '" + ScheduledTaskRunner.this.getScheduleName()
                            + "' with nodeName '" + Host.getLocalHostName() + "' "
                            + " is master and nextRun has passed now() exiting SLEEP_LOOP to do the schedule run ");
                    break SLEEP_LOOP;
                }

                // ?: Finally we have an option to pause the schedule, this works by only bypassing the run part and
                // doing a new sleep cycle to the next run.
                if (!ScheduledTaskRunner.this._active) {
                    // -> No, we have paused the schedule so we should skip this run and do another sleep cycle.
                    Instant nextRun = nextScheduledRun(getActiveCronExpressionInternal(), Instant.now(_clock));
                    log.info("Thread '" + ScheduledTaskRunner.this.getScheduleName()
                            + "' is currently deactivated so "
                            + "we are skipping this run and setting next run to '" + nextRun + "'");
                    _scheduledTaskRepository.updateNextRun(getScheduleName(), getOverrideExpressionAsString(), nextRun);
                    _isRunning = false;
                    _lastRunCompleted = Instant.now(_clock);
                    continue NEXTRUN;
                }

                // ----- We have not updated the cronExpression or the cronExpression is updated to be before the
                // previous set one. In any regards we have passed the moment where the schedule should run and we
                // have verified that we should not stop/skip the schedule
                log.info("Thread '" + ScheduledTaskRunner.this.getScheduleName()
                        + "' is beginning to do the run according "
                        + "to the set schedule '" + getActiveCronExpressionInternal().toString() + "'.");
                _currentRunStarted = Instant.now(_clock);
                ScheduleRunContext ctx = new ScheduledTaskRunnerContext(ScheduledTaskRunner.this,
                        createInstanceId(), _scheduledTaskRepository, _clock, _currentRunStarted);
                _scheduledTaskRepository.addScheduleRun(getScheduleName(), ctx.instanceId(),
                        _currentRunStarted, "Schedule run starting.");

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
                    String message = "Schedule '" + getScheduleName() + " run failed due to an error.' ";
                    log.info(message);
                    ctx.failed(message, t);
                }
                _lastRunCompleted = Instant.now(_clock);

                _isRunning = false;
                Instant nextRun = nextScheduledRun(getActiveCronExpressionInternal(), Instant.now(_clock));
                log.info("Thread '" + ScheduledTaskRunner.this.getScheduleName() + "' "
                        + " instanceId '" + ctx.instanceId() + "' "
                        + "used '" + Duration.between(_currentRunStarted, _lastRunCompleted).toMillis() + "' "
                        + "ms to run. Setting next run to '" + nextRun + "', "
                        + "using CronExpression '" + getActiveCronExpressionInternal() + "'");


                _scheduledTaskRepository.updateNextRun(getScheduleName(), getOverrideExpressionAsString(), nextRun);

                // :: Execute retention policy
                // TODO: Only execute at given intervals?
                _scheduledTaskRepository.executeRetentionPolicy(getScheduleName(), _config.getRetentionPolicy());
            }
            catch (Throwable t) {
                // ?: Check again for runThreads - as this will happen in shutdown
                if (!_runFlag) {
                    // -> Yes, we're stopping.
                    log.info("Thread '" + ScheduledTaskRunner.this.getScheduleName() + " got '" + t.getClass()
                            .getSimpleName()
                            + "' from the run-loop, but runThread-flag was false, so we're evidently exiting.");
                    _isRunning = false;
                    break;
                }

                log.warn("Thread '" + ScheduledTaskRunner.this.getScheduleName() + " got a '" + t.getClass()
                        .getSimpleName()
                        + "' from the run-loop. Chilling a little, then trying again.", t);
                try {
                    Thread.sleep(5000);
                }
                catch (InterruptedException e) {
                    log.info("Thread '" + ScheduledTaskRunner.this.getScheduleName() + " got interrupted while "
                            + "chill-waiting after an Exception when trying to do the schedule run"
                            + ". Ignoring by looping, thus checking runThread-flag.");
                }
            }
        }

        // Exiting loop, so clear the runner thread and log that we are now shutting down.
        _runner = null;
        log.info("Thread '" + ScheduledTaskRunner.this.getScheduleName() + "' asked to exit, shutting down!");
    }


    /**
     * Responsible of creating a unique runId for a given run.
     */
    private String createInstanceId() {
        return _currentRunStarted
                + "-" + getScheduleName()
                + "-" + Host.getLocalHostName();
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
        _scheduledTaskRepository.updateNextRun(getScheduleName(), newCronExpression,
                nextScheduledRun(parsedNewCronExpression, Instant.now(_clock)));
        // Updated cron, wake thread. The next run may be earlier than the previous set sleep cycle.
        notifyThread();
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
        _scheduledTaskRepository.setActive(getScheduleName(), isActive);
        _active = isActive;
    }

    @Override
    public String getScheduleName() {
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
        return _scheduledTaskRepository.getLastRunForSchedule(getScheduleName())
                .map(lastRun -> new ScheduledTaskRunnerContext(lastRun, this, _scheduledTaskRepository, _clock));
    }

    @Override
    public List<ScheduleRunContext> getAllScheduleRunsBetween(LocalDateTime from, LocalDateTime to) {
        return _scheduledTaskRepository.getScheduleRunsBetween(getScheduleName(), from, to).stream()
                .map(scheduledRunDto -> new ScheduledTaskRunnerContext(scheduledRunDto, this,
                        _scheduledTaskRepository, _clock))
                .collect(toList());

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
     * Shows if the current run is taking longer time than the {@link #_config#getMaxExpectedMinutesToRun()}.
     */
    @Override
    public boolean isOverdue() {
        Optional<Long> runTime = runTimeInMinutes();
        // ?: Did we get any runtime?
        if (!runTime.isPresent()) {
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
    public ScheduleRunContext getInstance(String instanceId) {
        Optional<ScheduledRunDto> scheduleRun = _scheduledTaskRepository.getScheduleRun(instanceId);

        // ?: Did we find the instance?
        if (scheduleRun.isPresent()) {
            // -> yes we found the schedule run
            return new ScheduledTaskRunnerContext(_scheduledTaskRepository.getScheduleRun(instanceId).get(),
                    this, _scheduledTaskRepository, _clock);
        }

        return null;
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
     *
     * @return
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
        // Write to db we should run this schedule now regardless of the cronExpression
        _scheduledTaskRepository.setRunOnce(getScheduleName(), true);
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
        private final Clock _clock;
        private final String _instanceId;
        private final ScheduledRunDto _scheduledRunDto;

        /**
         * Create a new {@link ScheduleRunContext}, IE a brand new run.
         */
        private ScheduledTaskRunnerContext(ScheduledTaskRunner storebrandSchedule, String instanceId,
                ScheduledTaskRepository scheduledTaskRepository, Clock clock, Instant runStart) {
            _storebrandSchedule = storebrandSchedule;
            _clock = clock;
            _instanceId = instanceId;
            _scheduledTaskRepository = scheduledTaskRepository;
            _scheduledRunDto = ScheduledRunDto.newWithStateStarted(storebrandSchedule.getScheduleName(), instanceId, runStart);
        }

        /**
         * Reload a previous run from the database. This run may also be ongoing meaning the schedule may still append
         * logs and set new statuses to the run.
         */
        private ScheduledTaskRunnerContext(ScheduledRunDto scheduledRunDto, ScheduledTaskRunner storebrandSchedule,
                ScheduledTaskRepository scheduledTaskRepository, Clock clock) {
            _storebrandSchedule = storebrandSchedule;
            _clock = clock;
            _scheduledTaskRepository = scheduledTaskRepository;
            _instanceId = scheduledRunDto.getInstanceId();
            _scheduledRunDto = scheduledRunDto;
        }


        @Override
        public String getScheduledName() {
            return _storebrandSchedule.getScheduleName();
        }

        @Override
        public String instanceId() {
            return _instanceId;
        }

        @Override
        public ScheduledTask getSchedule() {
            return _storebrandSchedule;
        }

        @Override
        public Instant getPreviousRun() {
            return _scheduledTaskRepository.getLastRunForSchedule(_storebrandSchedule.getScheduleName())
                    .map(scheduledRunDbo -> scheduledRunDbo.getRunStart())
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
            return _scheduledTaskRepository.getLogEntries(_instanceId);
        }

        @Override
        public void log(String msg) {
            _scheduledTaskRepository.addLogEntry(_instanceId, LocalDateTime.now(_clock), msg);
        }

        @Override
        public void log(String msg, Throwable throwable) {
            _scheduledTaskRepository.addLogEntry(_instanceId, LocalDateTime.now(_clock), msg,
                    throwableToStackTraceString(throwable));
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
