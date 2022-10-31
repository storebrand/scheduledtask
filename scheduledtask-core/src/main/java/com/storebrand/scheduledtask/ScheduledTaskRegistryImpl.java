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

import static java.util.stream.Collectors.toMap;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.storebrand.scheduledtask.ScheduledTask.Criticality;
import com.storebrand.scheduledtask.ScheduledTask.Recovery;
import com.storebrand.scheduledtask.ScheduledTask.RetentionPolicy;
import com.storebrand.scheduledtask.ScheduledTaskConfig.StaticRetentionPolicy;
import com.storebrand.scheduledtask.db.MasterLockRepository;
import com.storebrand.scheduledtask.db.ScheduledTaskRepository;
import com.storebrand.scheduledtask.SpringCronUtils.CronExpression;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Implementation of the {@link ScheduledTaskRegistry}
 *
 * @author Dag Bertelsen - dabe@skagenfondene.no - dabe@dagbertelsen.com - 2021.02
 * @author Kristian Hiim
 */
public class ScheduledTaskRegistryImpl implements ScheduledTaskRegistry {
    private static final Logger log = LoggerFactory.getLogger(ScheduledTaskRegistryImpl.class);
    private final Map<String, ScheduledTaskRunner> _schedules = new ConcurrentHashMap<>();

    private final MasterLockRepository _masterLockRepository;
    private final ScheduledTaskRepository _scheduledTaskRepository;
    private final boolean _testMode;
    private static final String MASTER_LOCK_NAME = "scheduledTask";
    private final MasterLockKeeper _masterLockKeeper;
    private final Clock _clock;
    private final List<ScheduledTaskListener> _scheduledTaskListeners = new CopyOnWriteArrayList<>();

    @SuppressFBWarnings("EI_EXPOSE_REP2")
    public ScheduledTaskRegistryImpl(ScheduledTaskRepository scheduledTaskRepository,
            MasterLockRepository masterLockRepository, Clock clock, boolean testMode) {
        _clock = clock;
        _masterLockRepository = masterLockRepository;
        _scheduledTaskRepository = scheduledTaskRepository;
        _testMode = testMode;

        _masterLockKeeper = new MasterLockKeeper(_masterLockRepository, this, clock);
        // ?: Are we running in test mode?
        if (!_testMode) {
            // -> No, then we start the MasterLockKeeper, so we try to get and keep the master lock.
            _masterLockKeeper.start();
        }
        else {
            // -> Yes, then we write a log message to let the world know.
            log.info("## TEST MODE ENABLED ## Scheduled tasks will only run if explicitly told to do so by calling"
                    + " ScheduledTask.runNow() - "
                    + " Background threads are disabled. This should only be enabled in unit tests.");
        }
    }

    public ScheduledTaskRegistryImpl(ScheduledTaskRepository scheduledTaskRepository,
            MasterLockRepository masterLockRepository, Clock clock) {
        this(scheduledTaskRepository, masterLockRepository, clock, false);
    }


    @Override
    public void close() {
        // Shutdown, loop through all treads and inform them we are shutting down.
        _schedules.entrySet().forEach(entry -> {
            log.info("Shutting down thread '" + entry.getKey() + "'");
            entry.getValue().killSchedule();
        });

        log.info("Shutting down the masterLock thread");
        _masterLockKeeper.stop();
    }

    @Override
    public void addListener(ScheduledTaskListener listener) {
        _scheduledTaskListeners.add(listener);
        // Notify listener about all scheduled tasks that have already been created before we added the listener.
        for (ScheduledTask scheduledTask : _schedules.values()) {
            listener.onScheduledTaskCreated(scheduledTask);
        }
    }

    @Override
    public ScheduledTaskBuilder buildScheduledTask(String name, String cronExpression,
            ScheduleRunnable runnable) {
        return new ScheduledTaskRunnerBuilder(name, cronExpression, runnable);
    }

    @Override
    public ScheduledTask getScheduledTask(String name) {
        if (name == null) {
            return null;
        }
        return _schedules.getOrDefault(name, null);
    }

    @Override
    public Map<String, ScheduledTask> getScheduledTasks() {
        return _schedules.entrySet().stream().collect(toMap(Entry::getKey, Entry::getValue));
    }

    @Override
    public Map<String, Schedule> getSchedulesFromRepository() {
        return _scheduledTaskRepository.getSchedules();
    }

    @Override
    public Optional<MasterLock> getMasterLock() {
        return _masterLockKeeper.getMasterLock();
    }

    @Override
    public boolean hasMasterLock() {
        return _masterLockKeeper.isMaster();
    }

    /**
     * Responsible of awaking all schedules.
     * Used by the {@link MasterLockKeeper} to awaken all nodes when it manages to aquire the master lock.
     */
    void wakeAllSchedules() {
        _schedules.entrySet().stream().forEach(entry -> {
            log.info("Awakening thread '" + entry.getKey() + "'");
            entry.getValue().notifyThread();
        });
    }

    void notifyScheduledTaskCreated(ScheduledTask scheduledTask) {
        for (ScheduledTaskListener listener : _scheduledTaskListeners) {
            listener.onScheduledTaskCreated(scheduledTask);
        }
    }

    boolean isTestMode() {
        return _testMode;
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
    static class MasterLockKeeper {
        private static final long MASTER_LOCK_SLEEP_LOOP_IN_MILLISECONDS = 2 * 60 * 1000; // 2 minutes
        private static final long MASTER_LOCK_MAX_TIME_SINCE_LAST_UPDATE_MINUTES = 5;

        private Thread _runner;
        private final Clock _clock;
        /* .. state vars .. */
        private volatile boolean _isMaster = false;
        private volatile Instant _lastUpdated = Instant.EPOCH;
        private volatile boolean _runFlag = true;
        private volatile boolean _isInitialRun = true;
        private final Object _syncObject = new Object();
        private final MasterLockRepository _masterLockRepository;
        private final ScheduledTaskRegistryImpl _storebrandScheduleService;
        private final AtomicInteger _notMasterCounter = new AtomicInteger();

        MasterLockKeeper(MasterLockRepository masterLockRepository,
                ScheduledTaskRegistryImpl storebrandScheduleService,
                Clock clock) {
            _masterLockRepository = masterLockRepository;
            _storebrandScheduleService = storebrandScheduleService;
            _clock = clock;
            log.info("Starting MasterLock thread");
            _runner = new Thread(MasterLockKeeper.this::runner, "MasterLock thread");

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
                            _lastUpdated = Instant.now(_clock);
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
                        _lastUpdated = Instant.now(_clock);
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
                        _lastUpdated = Instant.now(_clock);
                        _notMasterCounter.set(0);
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
                    _lastUpdated = Instant.now(_clock);
                    log.debug("Thread MasterLock '" + MASTER_LOCK_NAME + "', "
                            + " with nodeName '" + Host.getLocalHostName() + "' "
                            + "is not master.");

                    ensureMasterLockExists();

                    continue SLEEP_LOOP;
                }
                catch (InterruptedException e) {
                    log.info("MasterLock on node '" + Host.getLocalHostName() + "' sleep where interrupted."
                            + " Will loop and check run flag.");
                }
                catch (Throwable e) {
                    // We need to catch any and all exceptions thrown, so we ensure that the thread does not die on us.
                    log.error("Thread MasterLock '" + MASTER_LOCK_NAME
                            + "' failed with an exception. Will loop and try again.", e);
                }
            }
            // Exiting loop, so clear the runner thread and log that we are now shutting down.
            _runner = null;
            log.info("Thread MasterLock '" + MASTER_LOCK_NAME + "', "
                    + " with nodeName '" + Host.getLocalHostName() + "' "
                    + "asked to exit, shutting down!");
        }

        /**
         * The system should self-heal in case the master lock is gone from the database. This should normally never
         * happen, but if someone deletes the row we need to recreate it. If the row is gone no nodes will be able to
         * take the lock.
         * <p>
         * As an extra measure of protection against faults we will create the lock as if a non-existing node took it.
         * This way any node that thought it might have the lock will get enough time to let go before anyone is able to
         * pick it up.
         */
        private void ensureMasterLockExists() {
            // No need to check every cycle, so we check when we have not gotten the lock in 10 attempts, and reset
            // the counter if everything is good.
            int notMasterCounter = _notMasterCounter.incrementAndGet();
            if (notMasterCounter >= 10) {
                // We have not gotten the lock in 10 attempts, so we should check if the lock has gone missing, and
                // attempt to recreate the lock if that is the case.
                Optional<MasterLock> optionalMasterLock = getMasterLock();
                // ?: Is there a lock present?
                if (optionalMasterLock.isEmpty()) {
                    // -> No, there is no lock present, we must warn, and try to recreate it.
                    log.warn("Detected that MasterLock '" + MASTER_LOCK_NAME + "' is missing."
                            + " This should never happen. Attempting to recreate the missing lock.");
                    if (_masterLockRepository.tryCreateMissingLock(MASTER_LOCK_NAME)) {
                        log.info("Recreated missing MasterLock '" + MASTER_LOCK_NAME + "'");
                        // We recreated the lock row. Reset counter.
                        _notMasterCounter.set(0);
                    }
                    else {
                        log.warn("Unable to recreate missing MasterLock '" + MASTER_LOCK_NAME + "'. "
                                + "Maybe someone else created it?");
                    }
                    return;
                }

                // E-> A lock exists in the system. Just log a message with the current state

                MasterLock masterLock = optionalMasterLock.get();
                if (masterLock.isValid(Instant.now(_clock))) {
                    log.info("MasterLock '" + MASTER_LOCK_NAME + "' present, and taken by node '"
                            + masterLock.getNodeName() + "'");
                }
                else {
                    log.info("MasterLock '" + MASTER_LOCK_NAME + "' not taken. Last update was by '"
                            + masterLock.getNodeName() + "' at " + masterLock.getLockLastUpdatedTime());
                }
                // Lock row is present, so no need to check again for a while. Reset counter.
                _notMasterCounter.set(0);
            }
        }

        /**
         * We are only Master if both {@link #_isMaster} is true, and we were last updated less than
         * {@link #MASTER_LOCK_MAX_TIME_SINCE_LAST_UPDATE_MINUTES} minutes ago.
         */
        public boolean isMaster() {
            return _isMaster && _lastUpdated.isAfter(
                    Instant.now(_clock).minus(MASTER_LOCK_MAX_TIME_SINCE_LAST_UPDATE_MINUTES, ChronoUnit.MINUTES));
        }

        Optional<MasterLock> getMasterLock() {
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
            try {
                _runner.join(1000);
            }
            catch (InterruptedException e) {
                // Ignore interrupt here, we do best effort to wait for the runner thread to stop.
            }
            // Release the lock, only the node that is currently master are allowed to release it
            if (_masterLockRepository.releaseLock(MASTER_LOCK_NAME, Host.getLocalHostName())) {
                log.info("Thread MasterLock '" + MASTER_LOCK_NAME + "', "
                        + "with nodeName '" + Host.getLocalHostName() + "' "
                        + "are releasing the lock");

            }
        }
    }

    // ===== Helper class ==========================================================================================

    private class ScheduledTaskRunnerBuilder implements ScheduledTaskBuilder {

        private final String _scheduleName;
        private final String _cronExpression;
        private final ScheduleRunnable _runnable;
        private int _maxExpectedMinutesToRun = DEFAULT_MAX_EXPECTED_MINUTES_TO_RUN;
        private Criticality _criticality = DEFAULT_CRITICALITY;
        private Recovery _recovery = DEFAULT_RECOVERY;
        private int _deleteRunsAfter = DEFAULT_DELETE_RUNS_AFTER_DAYS;
        private int _deleteSuccessfulRunsAfter;
        private int _deleteFailedRunsAfterDays;
        private int _keepMaxRuns;
        private int _keepMaxSuccessfulRuns;
        private int _keepMaxFailedRuns;

        private ScheduledTaskRunnerBuilder(String scheduleName, String cronExpression,
                ScheduleRunnable runnable) {
            _scheduleName = scheduleName;
            _cronExpression = cronExpression;
            _runnable = runnable;
        }

        @Override
        public ScheduledTaskBuilder maxExpectedMinutesToRun(int minutes) {
            _maxExpectedMinutesToRun = minutes;
            return this;
        }

        @Override
        public ScheduledTaskBuilder criticality(Criticality criticality) {
            _criticality = criticality;
            return this;
        }

        @Override
        public ScheduledTaskBuilder recovery(Recovery recovery) {
            _recovery = recovery;
            return this;
        }

        @Override
        public ScheduledTaskBuilder deleteRunsAfterDays(int days) {
            _deleteRunsAfter = days;
            return this;
        }

        @Override
        public ScheduledTaskBuilder deleteSuccessfulRunsAfterDays(int days) {
            _deleteSuccessfulRunsAfter = days;
            return this;
        }

        @Override
        public ScheduledTaskBuilder deleteFailedRunsAfterDays(int days) {
            _deleteFailedRunsAfterDays = days;
            return this;
        }

        @Override
        public ScheduledTaskBuilder keepMaxRuns(int maxRuns) {
            _keepMaxRuns = maxRuns;
            return this;
        }

        @Override
        public ScheduledTaskBuilder keepMaxSuccessfulRuns(int maxSuccessfulRuns) {
            _keepMaxSuccessfulRuns = maxSuccessfulRuns;
            return this;
        }

        @Override
        public ScheduledTaskBuilder keepMaxFailedRuns(int maxFailedRuns) {
            _keepMaxFailedRuns = maxFailedRuns;
            return this;
        }

        @Override
        @SuppressFBWarnings(value = "NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE", justification = "cronExpressionParsed.next "
                + "always uses Temporal LocalDateTime and is always known.")
        public ScheduledTask start() {
            // In the first insert we can calculate the next run directly since there is no override from db yet
            CronExpression cronExpressionParsed = CronExpression.parse(_cronExpression);
            LocalDateTime nextRunTime = cronExpressionParsed.next(LocalDateTime.now(_clock));
            Instant nextRunInstant = nextRunTime.atZone(ZoneId.systemDefault()).toInstant();

            // Ensure schedule exists in database. This will only add the schedule if it does not exist.
            _scheduledTaskRepository.createSchedule(_scheduleName, nextRunInstant);

            ScheduledTask scheduledTask = _schedules.compute(_scheduleName, (key, value) -> {
                // ?: Do we already have a schedule with this name?
                if (value != null) {
                    // -> Yes, then we should throw so we don't create an additional runner for this schedule.
                    throw new DuplicateScheduledTaskException(key);
                }
                RetentionPolicy retentionPolicy = new StaticRetentionPolicy.Builder()
                        .deleteRunsAfterDays(_deleteRunsAfter)
                        .deleteSuccessfulRunsAfterDays(_deleteSuccessfulRunsAfter)
                        .deleteFailedRunsAfterDays(_deleteFailedRunsAfterDays)
                        .keepMaxRuns(_keepMaxRuns)
                        .keepMaxSuccessfulRuns(_keepMaxSuccessfulRuns)
                        .keepMaxFailedRuns(_keepMaxFailedRuns).build();

                ScheduledTaskConfig config = new ScheduledTaskConfig(_scheduleName, _cronExpression,
                        _maxExpectedMinutesToRun, _criticality, _recovery, retentionPolicy);

                return new ScheduledTaskRunner(config, _runnable, ScheduledTaskRegistryImpl.this,
                        _scheduledTaskRepository, _clock);
            });
            notifyScheduledTaskCreated(scheduledTask);
            return scheduledTask;
        }
    }
}
