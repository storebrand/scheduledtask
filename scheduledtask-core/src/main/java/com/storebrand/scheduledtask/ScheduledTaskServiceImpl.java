package com.storebrand.scheduledtask;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

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
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.storebrand.healthcheck.Axis;
import com.storebrand.healthcheck.CheckSpecification;
import com.storebrand.healthcheck.Responsible;
import com.storebrand.healthcheck.annotation.HealthCheck;
import com.storebrand.scheduledtask.db.ScheduledTaskRepository;
import com.storebrand.scheduledtask.db.ScheduledTaskRepository.ScheduleDto;
import com.storebrand.scheduledtask.db.ScheduledTaskRepository.ScheduledRunDto;
import com.storebrand.scheduledtask.SpringCronUtils.CronExpression;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Implementation of the {@link ScheduledTaskService}
 *
 * @author Dag Bertelsen - dabe@skagenfondene.no - dabe@dagbertelsen.com - 2021.02
 */
public class ScheduledTaskServiceImpl implements ScheduledTaskService {
    private static final Logger log = LoggerFactory.getLogger(ScheduledTaskServiceImpl.class);
    private final Map<String, ScheduleTaskImpl> _schedules = new ConcurrentHashMap<>();

    private static final String MONITOR_CHANGE_ACTIVE_PARAM = "toggleActive.local";
    private static final String MONITOR_EXECUTE_SCHEDULER = "executeScheduler.local";
    private static final String MONITOR_CHANGE_CRON = "changeCron.local";
    private static final String MONITOR_SHOW_RUNS = "showRuns.local";
    private static final String MONITOR_SHOW_LOGS = "showLogs.local";
    private static final String MONITOR_CHANGE_CRON_SCHEDULER_NAME = "changeCronSchedulerName.local";
    private static final String MASTER_LOCK_NAME = "scheduledTask";
    private static final long SLEEP_LOOP_MAX_SLEEP_AMOUNT_IN_MILLISECONDS = 2 * 60 * 1000; // 2 minutes
    private static final long MASTER_LOCK_SLEEP_LOOP_IN_MILLISECONDS = 2 * 60 * 1000; // 2 minutes
    private final MasterLock _masterLock;
    private final Clock _clock;
    private final MasterLockRepository _masterLockRepository;
    private final ScheduledTaskRepository _scheduledTaskRepository;

    @SuppressFBWarnings("EI_EXPOSE_REP2")
    public ScheduledTaskServiceImpl(ScheduledTaskRepository scheduledTaskRepository, DataSource dataSource, Clock clock) {
        _clock = clock;
        _masterLockRepository = new MasterLockRepository(dataSource, _clock);
        _scheduledTaskRepository = scheduledTaskRepository;

        _masterLock = new MasterLock(_masterLockRepository, this, clock);
        _masterLock.start();
    }


    @Override
    public void close() {
        // Shutdown, loop through all treads and inform them we are shutting down.
        _schedules.entrySet().stream().forEach(entry -> {
            log.info("Shutting down thread '" + entry.getKey() + "'");
            entry.getValue().killSchedule();
        });

        log.info("Shutting down the masterLock thread");
        _masterLock.stop();
    }

    /**
     * Create a new schedule that will run at the given {@link CronExpression}.
     *
     * @param schedulerName
     *         - Name of the schedule
     * @param cronExpression
     *         - When this schedule should run.
     * @param maxExpectedMinutes
     *         - Amount of minutes this should run.
     * @param runnable
     *         - The runnable that this schedule should run.
     */
    @Override
    @SuppressFBWarnings(value = "NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE", justification = "cronExpressionParsed.next "
            + "always uses Temporal LocalDateTime and is always known.")
    public ScheduledTask addSchedule(String schedulerName, String cronExpression, int maxExpectedMinutes,
            ScheduleRunnable runnable) {
        // In the first insert we can calculate the next run directly since there is no override from db yet
        CronExpression cronExpressionParsed = CronExpression.parse(cronExpression);
        LocalDateTime nextRunTime = cronExpressionParsed.next(LocalDateTime.now(_clock));
        Instant nextRunInstant = nextRunTime.atZone(ZoneId.systemDefault()).toInstant();

        // Add the schedule to the database
        _scheduledTaskRepository.createSchedule(schedulerName, nextRunInstant);

        // Create the schedule
        // TODO: Handle health checks
        ScheduleTaskImpl schedule = new ScheduleTaskImpl(schedulerName, cronExpression,
                Axis.DEGRADED_PARTIAL, _masterLock, _scheduledTaskRepository, maxExpectedMinutes, _clock, runnable);
        _schedules.put(schedulerName, schedule);
        return schedule;
    }

    /**
     * Create a new schedule that will run at the given {@link CronExpression}.
     *
     * @param schedulerName
     *         - Name of the schedule
     * @param cronExpression
     *         - When this schedule should run.
     * @param healthCheckLevel
     *         - If schedule fails what {@link Axis} should it report on {@link HealthCheck}.
     * @param maxExpectedMinutes
     *         - Amount of minutes this should run.
     * @param runnable
     *         - The runnable that this schedule should run.
     */
    // TODO: Handle healthchecks
    @SuppressFBWarnings(value = "NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE", justification = "cronExpressionParsed.next "
            + "always uses Temporal LocalDateTime and is always known.")
    public ScheduledTask addSchedule(String schedulerName, String cronExpression, Axis healthCheckLevel,
            int maxExpectedMinutes, ScheduleRunnable runnable) {
        // In the first insert we can calculate the next run directly since there is no override from db yet
        CronExpression cronExpressionParsed = CronExpression.parse(cronExpression);
        LocalDateTime nextRunTime = cronExpressionParsed.next(LocalDateTime.now(_clock));
        Instant nextRunInstant = nextRunTime.atZone(ZoneId.systemDefault()).toInstant();

        // Add the schedule to the database
        _scheduledTaskRepository.createSchedule(schedulerName, nextRunInstant);

        // Create the schedule
        ScheduleTaskImpl schedule = new ScheduleTaskImpl(schedulerName, cronExpression,
                healthCheckLevel, _masterLock, _scheduledTaskRepository, maxExpectedMinutes, _clock, runnable);
        _schedules.put(schedulerName, schedule);
        return schedule;
    }

    @Override
    public ScheduledTask getSchedule(String schedulerName) {
        if (schedulerName == null) {
            return null;
        }
        return _schedules.getOrDefault(schedulerName, null);
    }

    @Override
    public Map<String, ScheduledTask> getSchedules() {
        return _schedules.entrySet().stream().collect(toMap(Entry::getKey, Entry::getValue));
    }

    @Override
    public Optional<MasterLockDto> getMasterLock() {
        return _masterLock.getMasterLock();
    }

    @Override
    public boolean hasMasterLock() {
        return _masterLock.isMaster();
    }

    /**
     * Helper method that are used to stop a given schedule, will be triggering {@link ScheduledTask#stop()}
     * @see {@link ScheduledTask#stop()}
     */
    @Override
    public void stop(String schedulerName) {
        _schedules.computeIfPresent(schedulerName, (name, storebrandScheduled) -> {
            storebrandScheduled.stop();
            return storebrandScheduled;
        });
    }

    /**
     * Helper method that are used to start a given schedule, will be triggering {@link ScheduledTask#start()}
     * @see {@link ScheduledTask#start()}
     */
    @Override
    public void start(String schedulerName) {
        _schedules.computeIfPresent(schedulerName, (name, storebrandScheduled) -> {
            storebrandScheduled.start();
            return storebrandScheduled;
        });
    }

    /**
     * Helper method that are used to run given schedule as soon as possbile. Will be triggering {@link ScheduledTask#runNow()}
     * @see {@link ScheduledTask#runNow()}
     */
    @Override
    public void runNow(String schedulerName) {
        _schedules.computeIfPresent(schedulerName, (name, storebrandScheduled) -> {
            storebrandScheduled.runNow();
            return storebrandScheduled;
        });
    }

    /**
     * Responsible of awaking all schedules.
     * Used by the {@link MasterLock} to awaken all nodes when it manages to aquire the master lock.
     */
    void wakeAllSchedules() {
        _schedules.entrySet().stream().forEach(entry -> {
            log.info("Awakening thread '" + entry.getKey() + "'");
            entry.getValue().notifyThread();
        });

    }

    // ===== ServerStatus and Monitor ==================================================================================
    @HealthCheck(name = "Master lock")
    public void masterLockServerStatus(CheckSpecification spec) {
        spec.dynamicText(context -> "Running node has the master lock: " + (_masterLock.isMaster() ? "yes" : "no"));
        // :: Get the status from the db on who has the lock
        spec.check(Responsible.DEVELOPERS, Axis.DEGRADED_PARTIAL, checkContext -> {
                    Optional<MasterLockDto> masterLock = _masterLock.getMasterLock();
                    // ?: Is the lock yet unclaimed?
                    if (!masterLock.isPresent()) {
                        // -> Yes, nobody has the lock. This means this node is just starting and have not managed
                        // to claim the lock on the first run yet.
                        return checkContext.ok("Unclaimed lock");
                    }

                    // ?: We have a lock but it may be old
                    if (!masterLock.get().isValid(_clock.instant())) {
                        // Yes-> it is an old lock
                        return checkContext.fault("No-one currently has the lock, last node to have it was "
                                + "[" + masterLock.get().getNodeName() + "]. It was last updated "
                                + "[" + LocalDateTime.ofInstant(masterLock.get().getLockLastUpdatedTime(), ZoneId.systemDefault()) + "]");
                    }

                    // ----- Lock flag in db is valid.
                    return checkContext.ok("The node [" + masterLock.get().getNodeName() + "] has the lock! "
                            + "It was acquired "
                            + "[" + LocalDateTime.ofInstant(masterLock.get().getLockTakenTime(), ZoneId.systemDefault()) + "] "
                            + "and it was last updated ["
                            + LocalDateTime.ofInstant(masterLock.get().getLockLastUpdatedTime(), ZoneId.systemDefault()) + "]");
                });
    }

    @HealthCheck(name = "Scheduled tasks")
    public void storebrandSchedulesServerStatus(CheckSpecification spec) {
        spec.check(Responsible.DEVELOPERS, Axis.DEGRADED_MINOR, checkContext -> {
                    Map<String, ScheduleDto> allSchedulesFromDb = _scheduledTaskRepository.getSchedules().stream()
                            .collect(toMap(ScheduledTaskRepository.ScheduleDto::getScheduleName, scheduleDto -> scheduleDto));
                    TableBuilder tableBuilder = new TableBuilder(
                            "Schedule",
                            "Active",
                            "Default cron",
                            "Running",
                            "Overdue",
                            "Status");
                    boolean failed = false;
                    for (Entry<String, ScheduleTaskImpl> entry : _schedules.entrySet()) {
                        boolean scheduleFailed;
                        // Is this schedule active?
                        boolean isActive = allSchedulesFromDb.get(entry.getKey()).isActive();
                        scheduleFailed = !isActive;

                        // has overridden CronExpression / default cron
                        boolean defaultCron = !allSchedulesFromDb.get(entry.getKey())
                                .getOverriddenCronExpression().isPresent();
                        scheduleFailed = scheduleFailed || !defaultCron;

                        // Is this schedule overdue. This is stored in the memory version of the current run.
                        // It is only overdue if it is active, is running and marked as overdue.
                        boolean isOverdue = entry.getValue().isOverdue()
                                && entry.getValue().isActive()
                                && entry.getValue().isRunning();
                        scheduleFailed = scheduleFailed || isOverdue;

                        // Add a table row
                        tableBuilder.addRow(entry.getKey(),
                                isActive,
                                defaultCron,
                                entry.getValue().isRunning(),
                                isOverdue,
                                scheduleFailed ? "WARN" : "OK");
                        failed = failed || scheduleFailed;
                    }
                    checkContext.text(tableBuilder.toString());
                    return checkContext.faultConditionally(failed, "Schedules status");
                });

        // Add a blank line to add som "air" between the two section
        spec.staticText("");
        // Render out based on the set Healthlevel. We check the lastRun to see if this failed or if the schedule is
        // overdue times 10. If the schedule is overdue or last run failed we render ONE line for each error and
        // set the defined getHealthCheckLevel() for this schedule.
        for (Entry<String, ScheduleTaskImpl> entry : _schedules.entrySet()) {
            spec.check(Responsible.DEVELOPERS, entry.getValue().getHealthCheckLevel(), checkContext -> {
                Optional<ScheduledRunDto> scheduleFromDb =
                        _scheduledTaskRepository.getLastRunForSchedule(entry.getKey());
                boolean runFailed = false;
                // :? Did the last run fail?
                if (scheduleFromDb.isPresent() && State.FAILED.equals(scheduleFromDb.get().getStatus())) {
                    // -> Yes this status failed
                    runFailed = true;
                    checkContext.text(entry.getKey() + " last run failed!");
                }

                Long runTime = entry.getValue().runTimeInMinutes().orElse(0L);
                int wayOverdueTime = 10 * entry.getValue().getMaxExpectedMinutesToRun();
                // :? Is this schedule way overdue?
                if (runTime > 10 * wayOverdueTime) {
                    // -> Yes this run has taken 10 times the estimated amount so we should display the HealthCheckLevel
                    runFailed = true;
                    checkContext.text(entry.getKey() + " is taking more than 10x the estimated runtime!");
                }

                return checkContext.faultConditionally(runFailed, entry.getKey() + " last run status");

            });
        }
    }

    // ===== MasterLock and Schedule ===================================================================================

    /**
     * Master lock is responsible for acquiring and keeping a lock, the node that has this lock will be responsible
     * for running ALL Schedules.
     * When a lock is first acquired this node has this for 5 minutes and in order to keep it this node has to the
     * method {@link MasterLockRepository#keepLock(String, String)} within that timespan in order to keep it for a new
     * 5 minutes. If this node does not manage to keep the lock within the 5 min timespan it means no nodes are master
     * for the duration 5 - 10 min.
     * After the lock has not been updated for 10 minutes it can be re-acquired again.
     */
    static class MasterLock {
        private Thread _runner;
        private final Clock _clock;
        /* .. state vars .. */
        private volatile boolean _isMaster = false;
        private volatile boolean _runFlag = true;
        private volatile boolean _isInitialRun = true;
        private final Object _syncObject = new Object();
        private final MasterLockRepository _masterLockRepository;
        private final ScheduledTaskServiceImpl _storebrandScheduleService;

        MasterLock(MasterLockRepository masterLockRepository,
                ScheduledTaskServiceImpl storebrandScheduleService,
                Clock clock) {
            _masterLockRepository = masterLockRepository;
            _storebrandScheduleService = storebrandScheduleService;
            _clock = clock;
            log.info("Starting MasterLock thread");
            _runner = new Thread(() -> MasterLock.this.runner(), "MasterLock thread");

            // Make sure that the master lock is created:
            _masterLockRepository.tryCreateLock(MASTER_LOCK_NAME, Host.getLocalHostName());
        }

        void runner() {
            SLEEP_LOOP: while (_runFlag) {
                try {
                    // ?: is this an initial run?
                    if (_isInitialRun) {
                        // Yes -> This is an initial run so we should assume that no node currently has the lock.
                        // We need to first let all nodes try to acquire the lock before we sleep so the nodes know
                        // who is the master node. If we don't check this here then it will go a full sleep where no
                        // nodes know who is the master node.
                        if (_masterLockRepository.tryAcquireLock(MASTER_LOCK_NAME, Host.getLocalHostName())) {
                            // -> Yes, we managed to acquire the lock
                            _isMaster = true;
                            log.info("Thread MasterLock '" + MASTER_LOCK_NAME + "', "
                                    + " with nodeName '" + Host.getLocalHostName() + "' "
                                    + "managed to acquire the lock during the initial run");
                        }
                        // Regardless if we managed to acquire the lock we have passed the initial run.
                        _isInitialRun = false;
                    }

                    // :: Sleep a bit before we attempts to keep/acquire the lock
                    synchronized (_syncObject) {
                        log.debug("Thread MasterLock '" + MASTER_LOCK_NAME + "' sleeping, "
                                + " with nodeName '" + Host.getLocalHostName() + "' "
                                + "is going to sleep for '" + MASTER_LOCK_SLEEP_LOOP_IN_MILLISECONDS + "' ms.");
                        _syncObject.wait(MASTER_LOCK_SLEEP_LOOP_IN_MILLISECONDS);
                    }

                    // :: Try to keep the lock, this will only succeed if the lock is already acquired for this node
                    // withing the last 5 minutes.
                    if (_masterLockRepository.keepLock(MASTER_LOCK_NAME, Host.getLocalHostName())) {
                        // -> Yes, we managed to keep the master lock, go to sleep
                        _isMaster = true;
                        log.info("Thread MasterLock '" + MASTER_LOCK_NAME + "', "
                                + " with nodeName '" + Host.getLocalHostName() + "' "
                                + "managed to keep the lock.");
                        continue SLEEP_LOOP;
                    }

                    // ----- We are not master, either we did not manage to keep the lock or we have never gotten it,
                    // so try to acquire the lock.;
                    // ?: Did we manage to acquire the lock.
                    if (_masterLockRepository.tryAcquireLock(MASTER_LOCK_NAME, Host.getLocalHostName())) {
                        // -> Yes, we managed to acquire the lock
                        _isMaster = true;
                        log.info("Thread MasterLock '" + MASTER_LOCK_NAME + "', "
                                + " with nodeName '" + Host.getLocalHostName() + "' "
                                + "managed to acquire the lock!");
                        // Also wake all schedules on this host since these may sleep and does not yet know they are
                        // the now master node
                        _storebrandScheduleService.wakeAllSchedules();
                        continue SLEEP_LOOP;
                    }

                    // ----- We where not able to keep the lock and not able to acquire the lock so we are not master,
                    // but we should regardless of this do a new sleep cycle after we mark us as not the master
                    _isMaster = false;
                    log.debug("Thread MasterLock '" + MASTER_LOCK_NAME + "', "
                            + " with nodeName '" + Host.getLocalHostName() + "' "
                            + "is not master.");
                    continue SLEEP_LOOP;
                }
                catch (InterruptedException e) {
                    log.debug("MasterLock on node '" + Host.getLocalHostName() + "' sleep where interrupted");
                }
            }
            // Exiting loop, so clear the runner thread and log that we are now shutting down.
            _runner = null;
            log.info("Thread MasterLock '" + MASTER_LOCK_NAME + "', "
                    + " with nodeName '" + Host.getLocalHostName() + "' "
                    + "asked to exit, shutting down!");
        }

        public boolean isMaster() {
            return _isMaster;
        }

        Optional<MasterLockDto> getMasterLock() {
            return _masterLockRepository.getLock(MASTER_LOCK_NAME);
        }

        void start() {
            _runFlag = true;
            _isInitialRun = true;
            _runner.start();
        }

        void stop() {
            _runFlag = false;
            synchronized (_syncObject) {
                _syncObject.notifyAll();
            }
            // Release the lock, only the node that is currently master are allowed to release it
            if (_masterLockRepository.releaseLock(MASTER_LOCK_NAME, Host.getLocalHostName())) {
                log.info("Thread MasterLock '" + MASTER_LOCK_NAME + "', "
                        + " with nodeName '" + Host.getLocalHostName() + "' "
                        + " are releasing the lock");

            }
        }
    }

    /**
     * The schedule class that is responsible of handling when it should run, sleep, pause and shutting down.
     */
    static class ScheduleTaskImpl implements ScheduledTask {
        private final String _schedulerName;
        private Thread _runner;
        private final ScheduleRunnable _runnable;
        private final CronExpression _defaultCronExpression;
        private final int _maxExpectedMinutesToRun;
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
        private final MasterLock _masterLock;
        private final ScheduledTaskRepository _scheduledTaskRepository;
        private final Axis _healthCheckLevel;
        private final Clock _clock;

        ScheduleTaskImpl(String schedulerName, String cronExpression,
                Axis healthCheckLevel, MasterLock masterLock, ScheduledTaskRepository scheduledTaskRepository,
                int maxExpectedMinutes, Clock clock, ScheduleRunnable runnable) {
            _clock = clock;
            _defaultCronExpression = CronExpression.parse(cronExpression);
            _healthCheckLevel = healthCheckLevel;
            _maxExpectedMinutesToRun = maxExpectedMinutes;
            _schedulerName = schedulerName;
            _masterLock = masterLock;
            _scheduledTaskRepository = scheduledTaskRepository;
            _runnable = runnable;
            log.info("Starting Thread '" + schedulerName + "' with the cronExpression '" + cronExpression + "'.");
            _runner = new Thread(() -> ScheduleTaskImpl.this.runner(),
                    "ScheduledTask thread '" + schedulerName + "'");
            _runner.start();

        }

        @SuppressWarnings({ "checkstyle:IllegalCatch", "MethodLength", "PMD.AvoidBranchingStatementAsLastInLoop" })
        // We want to log everything
        void runner() {
            NEXTRUN: while (_runFlag) {
                try {
                    SLEEP_LOOP: while (_runFlag) {
                        // get the next run from the db:
                        Optional<ScheduleDto> scheduleFromDb = _scheduledTaskRepository.getSchedule(_schedulerName);
                        if (!scheduleFromDb.isPresent()) {
                            throw new RuntimeException("Schedule with name '" + _schedulerName + " was not found'");
                        }

                        _nextRun = scheduleFromDb.get().getNextRun();
                        // We may have set an override expression, so retrieve this and store it for use when we
                        // should update the nextRun
                        _overrideExpression = scheduleFromDb.get().getOverriddenCronExpression()
                                .map(CronExpression::parse)
                                .orElse(null);

                        // ?: Are we the master node
                        if (_masterLock.isMaster()) {
                            // -> Yes, we are master and should only sleep if we are still waiting for the nextRun.
                            if (Instant.now(_clock).isBefore(_nextRun)) {
                                // -> No, we have not yet passed the next run so we should sleep a bit
                                long millisToWait = ChronoUnit.MILLIS.between(Instant.now(_clock), _nextRun);
                                // We should only sleep only max SLEEP_LOOP_MAX_SLEEP_AMOUNT_IN_MILLISECONDS in
                                // one sleeop loop.
                                millisToWait = millisToWait > SLEEP_LOOP_MAX_SLEEP_AMOUNT_IN_MILLISECONDS
                                        ? SLEEP_LOOP_MAX_SLEEP_AMOUNT_IN_MILLISECONDS
                                        : millisToWait;
                                synchronized (_syncObject) {
                                    log.debug("Thread '" + ScheduleTaskImpl.this._schedulerName + "', "
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
                                log.info("Thread '" + ScheduleTaskImpl.this._schedulerName + "', "
                                        + " slave node '" + Host.getLocalHostName() + "' "
                                        + "is going to sleep for 15 min.");
                                _syncObject.wait(15 * 60 * 1_000);
                            }
                        }

                        // We have exited the sleep.
                        log.debug("Thread '" + ScheduleTaskImpl.this._schedulerName
                                + "' with nodeName '" + Host.getLocalHostName() + "' "
                                + " exited the sleep.");
                        _isRunning = true;

                        // ?: Check again for runThreads - as this will happen in shutdown
                        if (!_runFlag) {
                            // -> Yes, we're stopping.
                            log.info("Thread '" + ScheduleTaskImpl.this._schedulerName
                                    + "' with nodeName '" + Host.getLocalHostName() + "' "
                                    + " is exiting the schedule loop.");
                            _isRunning = false;
                            break NEXTRUN;
                        }

                        // ?: Are we master
                        if (!_masterLock.isMaster()) {
                            Instant nextRun = nextScheduledRun(getActiveCronExpressionInternal(), Instant.now(_clock));

                            log.info("Thread '" + ScheduleTaskImpl.this._schedulerName
                                    + "' with nodeName '" + Host.getLocalHostName() + "' "
                                    + " is not currently master so restarting sleep loop, next run is set"
                                    + " to '" + nextRun + "'");
                            _isRunning = false;
                            continue SLEEP_LOOP;
                        }

                        // Re-retrieve the schedule from the db. It may have changed during the sleep:
                        scheduleFromDb = _scheduledTaskRepository.getSchedule(_schedulerName);
                        if (!scheduleFromDb.isPresent()) {
                            throw new RuntimeException("Schedule with name '" + _schedulerName + " was not found'");
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
                            _scheduledTaskRepository.setRunOnce(_schedulerName, false);
                            log.info("Thread '" + ScheduleTaskImpl.this._schedulerName
                                    + "' with nodeName '" + Host.getLocalHostName() + "' "
                                    + " is set to run once (NOW) and then continue as set in "
                                    + "schedule '" + getActiveCronExpressionInternal().toString() + "'.");
                            break SLEEP_LOOP;
                        }

                        // ?: Have we passed the nextRun instance
                        if (Instant.now(_clock).isBefore(_nextRun)) {
                            // -> No, we have not yet passed the nextRun (now is still before nextRun)
                            // so we should do another sleep cycle.
                            log.debug("Thread '" + ScheduleTaskImpl.this._schedulerName
                                    + "' with nodeName '" + Host.getLocalHostName() + "' "
                                    + " has to sleep a bit longer "
                                    + "since we have not passed the nextRun instance.");
                            _isRunning = false;
                            continue SLEEP_LOOP;
                        }

                        // ----- _runFlag is still true, we are master, and nextRun is after the now(),
                        // exit the SLEEP_LOOP And run the schedule
                        log.debug("Thread '" + ScheduleTaskImpl.this._schedulerName
                                + "' with nodeName '" + Host.getLocalHostName() + "' "
                                + " is master and nextRun has passed now() exiting SLEEP_LOOP to do the schedule run ");
                        break SLEEP_LOOP;
                    }

                    // ?: Finally we have an option to pause the schedule, this works by only bypassing the run part and
                    // doing a new sleep cycle to the next run.
                    if (!ScheduleTaskImpl.this._active) {
                        // -> No, we have paused the schedule so we should skip this run and do another sleep cycle.
                        Instant nextRun = nextScheduledRun(getActiveCronExpressionInternal(), Instant.now(_clock));
                        log.info("Thread '" + ScheduleTaskImpl.this._schedulerName
                                + "' is currently deactivated so "
                                + "we are skipping this run and setting next run to '" + nextRun + "'");
                        _scheduledTaskRepository.updateNextRun(_schedulerName, getOverrideExpressionAsString(), nextRun);
                        _isRunning = false;
                        _lastRunCompleted = Instant.now(_clock);
                        continue NEXTRUN;
                    }

                    // ----- We have not updated the cronExpression or the cronExpression is updated to be before the
                    // previous set one. In any regards we have passed the moment where the schedule should run and we
                    // have verified that we should not stop/skip the schedule
                    log.info("Thread '" + ScheduleTaskImpl.this._schedulerName
                            + "' is beginning to do the run according "
                            + "to the set schedule '" + getActiveCronExpressionInternal().toString() + "'.");
                    _currentRunStarted = Instant.now(_clock);
                    ScheduleRunContext ctx = new MyScheduleRunContext(ScheduleTaskImpl.this,
                            createInstanceId(), _scheduledTaskRepository, _clock, _currentRunStarted);
                        _scheduledTaskRepository.addScheduleRun(_schedulerName, ctx.instanceId(),
                                _currentRunStarted, "Schedule run starting.");

                    // ?: Is this schedule manually triggered? Ie set to run once.
                    if (ScheduleTaskImpl.this._runOnce) {
                        // -> Yes, this where set to run once and is manually triggered. so add a log line.
                        // note, this is named runNow in the gui but runOnce in the db so we use the term used
                        // in gui for the logging.
                        ctx.log("Manually started");
                    }

                    // :: Try to run the code that the user wants, log if it fails.
                    try {
                        ScheduleStatus status = _runnable.run(ctx);
                        if (!(status instanceof ScheduleStatusImpl)) {
                            throw new IllegalArgumentException("The ScheduleRunContext returned a invalid ScheduleStatus,"
                                    + " make sure you are using done(), failed() or dispatched()");
                        }
                    }
                    catch (Throwable t) {
                        // Oh no, the schedule we set to run failed, we should log this and continue
                        // on the normal schedule loop
                        String message = "Schedule '" + _schedulerName + " run failed due to an error.' ";
                        log.info(message);
                        ctx.failed(message, t);
                    }
                    _lastRunCompleted = Instant.now(_clock);

                    _isRunning = false;
                    Instant nextRun = nextScheduledRun(getActiveCronExpressionInternal(), Instant.now(_clock));
                    log.info("Thread '" + ScheduleTaskImpl.this._schedulerName + "' "
                            + " instanceId '" + ctx.instanceId() + "' "
                            + "used '" + Duration.between(_currentRunStarted, _lastRunCompleted).toMillis() + "' "
                            + "ms to run. Setting next run to '" + nextRun + "', "
                            + "using CronExpression '" + getActiveCronExpressionInternal() + "'");


                    _scheduledTaskRepository.updateNextRun(_schedulerName, getOverrideExpressionAsString(), nextRun);
                }
                catch (Throwable t) {
                    // ?: Check again for runThreads - as this will happen in shutdown
                    if (!_runFlag) {
                        // -> Yes, we're stopping.
                        log.info("Thread '" + ScheduleTaskImpl.this._schedulerName + " got '" + t.getClass()
                                .getSimpleName()
                                + "' from the run-loop, but runThread-flag was false, so we're evidently exiting.");
                        _isRunning = false;
                        break;
                    }

                    log.warn("Thread '" + ScheduleTaskImpl.this._schedulerName + " got a '" + t.getClass()
                            .getSimpleName()
                            + "' from the run-loop. Chilling a little, then trying again.", t);
                    try {
                        Thread.sleep(5000);
                    }
                    catch (InterruptedException e) {
                        log.info("Thread '" + ScheduleTaskImpl.this._schedulerName + " got interrupted while "
                                + "chill-waiting after an Exception when trying to do the schedule run"
                                + ". Ignoring by looping, thus checking runThread-flag.");
                    }
                }
            }

            // Exiting loop, so clear the runner thread and log that we are now shutting down.
            _runner = null;
            log.info("Thread '" + ScheduleTaskImpl.this._schedulerName + "' asked to exit, shutting down!");
        }

        // ===== Helper class ==========================================================================================


        /**
         * Get the {@link Axis} this should use if the {@link HealthCheck} fails for this schedule.
         */
        public Axis getHealthCheckLevel() {
            return _healthCheckLevel;
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
         * The cron expression currently set on the scheduler. Usually we only use the {@link #_defaultCronExpression}
         * but we can override this in the monitor so we should check if the {@link #_overrideExpression} have been
         * set.
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
         * The cron expression currently set on the scheduler. Usually we only use the {@link #_defaultCronExpression}
         * but we can override this in the monitor so we should check if the {@link #_overrideExpression} have been
         * set.
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
         * Set a new cronExpression to be used by the schedule. If this is set to null it will fall back to use the
         * {@link #_defaultCronExpression} again.
         */
        @Override
        public void setOverrideExpression(String newCronExpression) {
            CronExpression parsedNewCronExpression = newCronExpression != null
                    ? CronExpression.parse(newCronExpression) : null;
            _overrideExpression = parsedNewCronExpression;
            _scheduledTaskRepository.updateNextRun(_schedulerName, newCronExpression,
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
            _scheduledTaskRepository.setActive(_schedulerName, isActive);
            _active = isActive;
        }

        @Override
        public String getScheduleName() {
            return _schedulerName;
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
        public ScheduleRunContext getLastScheduleRun() {
            return new MyScheduleRunContext(_scheduledTaskRepository.getLastRunForSchedule(_schedulerName).get(),
                    this, _scheduledTaskRepository, _clock);
        }

        @Override
        public List<ScheduleRunContext> getAllScheduleRunsBetween(LocalDateTime from, LocalDateTime to) {
            return _scheduledTaskRepository.getScheduleRunsBetween(_schedulerName, from, to).stream()
                    .map(scheduledRunDto -> new MyScheduleRunContext(scheduledRunDto, this,
                            _scheduledTaskRepository, _clock))
                    .collect(toList());

        }

        /**
         * Shows if the current schedule is currently running.
         * This will be set to <i>true</i> when a scheduled run starts and to <i>false</i> when it stops.
         * <p>
         * This together with {@link #isOverdue()} shows if the current schedule is alive and working as expected.
         */
        @Override
        public boolean isRunning() {
            return _isRunning;
        }

        /**
         * Shows if the current run is taking longer time than the {@link #_maxExpectedMinutesToRun}.
         */
        @Override
        public boolean isOverdue() {
            Optional<Long> runTime = runTimeInMinutes();
            // ?: Did we get any runtime?
            if (!runTime.isPresent()) {
                // -> No, so it is not overdue
                return false;
            }
            return runTime.get() >= _maxExpectedMinutesToRun;
        }

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
            return _maxExpectedMinutesToRun;
        }

        @Override
        public ScheduleRunContext getInstance(String instanceId) {
            Optional<ScheduledRunDto> scheduleRun = _scheduledTaskRepository.getScheduleRun(instanceId);

            // ?: Did we find the instance?
            if (scheduleRun.isPresent()) {
                // -> yes we found the schedule run
                return new MyScheduleRunContext(_scheduledTaskRepository.getScheduleRun(instanceId).get(),
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
         * The name of the schedule.
         */
        String getSchedulerName() {
            return _schedulerName;
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
            _scheduledTaskRepository.setRunOnce(_schedulerName, true);
            // Then wake the tread so it is aware that we should run now once
            synchronized (_syncObject) {
                _syncObject.notifyAll();
            }
        }
    }

    /**
     * Implementation of the {@link ScheduleRunContext}, it is indirectly used when a schedule is running and
     * this run is appending {@link #log(String)}, {@link #dispatched(String)}, {@link #done(String)}
     * or {@link #failed(String)}
     */
    private static class MyScheduleRunContext implements ScheduleRunContext {
        private final ScheduleTaskImpl _storebrandSchedule;
        private final ScheduledTaskRepository _scheduledTaskRepository;
        private final Clock _clock;
        private final String _instanceId;
        private final ScheduledRunDto _scheduledRunDto;

        /**
         * Create a new {@link ScheduleRunContext}, IE a brand new run.
         */
        MyScheduleRunContext(ScheduleTaskImpl storebrandSchedule, String instanceId,
                ScheduledTaskRepository scheduledTaskRepository, Clock clock, Instant runStart) {
            _storebrandSchedule = storebrandSchedule;
            _clock = clock;
            _instanceId = instanceId;
            _scheduledTaskRepository = scheduledTaskRepository;
            _scheduledRunDto = ScheduledTaskRepository.ScheduledRunDto.newWithStateStarted(storebrandSchedule.getScheduleName(), instanceId, runStart);
        }

        /**
         * Reload a previous run from the database. This run may also be on going meaning the schedule may still append
         * logs and set new statuses to the run.
         */
        private MyScheduleRunContext(ScheduledRunDto scheduledRunDto, ScheduleTaskImpl storebrandSchedule,
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
            return _scheduledTaskRepository.getLastRunForSchedule(_storebrandSchedule._schedulerName)
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
            _scheduledTaskRepository.addLogEntry(_instanceId, new LogEntry(msg, LocalDateTime.now(_clock)));
        }

        @Override
        public void log(String msg, Throwable throwable) {
            _scheduledTaskRepository.addLogEntry(_instanceId,
                    new LogEntry(msg, throwableToString(throwable), LocalDateTime.now(_clock)));
        }

        @Override
        public ScheduleStatus done(String msg) {
            _scheduledRunDto.setStatus(State.DONE, Instant.now(_clock), msg);
            _scheduledTaskRepository.setStatus(_scheduledRunDto);
            log("[" + State.DONE + "] " + msg);
            return new ScheduleStatusImpl();
        }

        @Override
        public ScheduleStatus failed(String msg) {
            _scheduledRunDto.setStatus(State.FAILED, Instant.now(_clock), msg);
            _scheduledTaskRepository.setStatus(_scheduledRunDto);
            log("[" + State.FAILED + "] " + msg);
            return new ScheduleStatusImpl();
        }

        @Override
        public ScheduleStatus failed(String msg, Throwable throwable) {
            _scheduledRunDto.setStatus(State.FAILED, Instant.now(_clock), msg, throwableToString(throwable));
            _scheduledTaskRepository.setStatus(_scheduledRunDto);
            log("[" + State.FAILED + "] " + msg, throwable);
            return new ScheduleStatusImpl();
        }

        @Override
        public ScheduleStatus dispatched(String msg) {
            _scheduledRunDto.setStatus(State.DISPATCHED, Instant.now(_clock), msg);
            _scheduledTaskRepository.setStatus(_scheduledRunDto);
            log("[" + State.DISPATCHED + "] " + msg);
            return new ScheduleStatusImpl();
        }
    }

    /**
     * NO-OP class only used to make sure the users are calling the {@link ScheduleRunContext#done(String)}, {@link
     * ScheduleRunContext#failed(String)} or {@link ScheduleRunContext#dispatched(String)}
     */
    private static class ScheduleStatusImpl implements ScheduleStatus {

    }

    // ===== Helpers===========================================================================
    /**
     * Converts a throwable to string.
     */
    static String throwableToString(Throwable throwable) {
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
