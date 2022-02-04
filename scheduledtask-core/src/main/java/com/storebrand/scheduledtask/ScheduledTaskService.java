package com.storebrand.scheduledtask;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.storebrand.healthcheck.Axis;
import com.storebrand.scheduledtask.ScheduledTaskServiceImpl.MasterLockDto;
import com.storebrand.scheduledtask.ScheduledTaskServiceImpl.State;
import com.storebrand.scheduledtask.internal.cron.CronExpression;

/**
 * ScheduledTask methods for the scheduler service.
 *
 * @author Dag Bertelsen - dag.lennart.bertelsen@storebrand.no - dabe@dagbertelsen.com - 2021.03
 */
public interface ScheduledTaskService {

    /**
     * Create a new schedule that will run at the given {@link CronExpression}.
     *
     * @param scheduleName
     *         - Name of the schedule
     * @param cronExpression
     *         - When this schedule should run.
     * @param runnable
     *         - The runnable that this schedule should run.
     */
    ScheduledTask addSchedule(String scheduleName, String cronExpression, Axis healthCheckLevel, int maxExpectedMinutes, ScheduleRunnable runnable);

    /**
     * Get a schedule with a specific name.
     */
    ScheduledTask getSchedule(String scheduleName);

    /**
     * Get a copy of all the schedules that are registered in the system.
     */
    Map<String, ScheduledTask> getSchedules();

    /**
     * Helper method that are used to start a given schedule, will be triggering {@link ScheduledTask#start()}
     * @see {@link ScheduledTask#start()}
     */
    void start(String schedulerName);

    /**
     * Helper method that are used to stop a given schedule, will be triggering {@link ScheduledTask#stop()}
     * @see {@link ScheduledTask#stop()}
     */
    void stop(String schedulerName);

    /**
     * Helper method that are used to run given schedule as soon as possbile. Will be triggering {@link ScheduledTask#runNow()}
     * @see {@link ScheduledTask#runNow()}
     */
    void runNow(String schedulerName);

    /**
     * Get information if any node currently has the master lock and if so what node currently has it. There is
     * also a chance that the lock is not set to anyone and this return who had it last, to figure out if this
     * returns an old lock you need to check {@link MasterLockDto#getLockLastUpdatedTime()} by the following rules:
     * <ol>
     *     <li>If the lock {@link MasterLockDto#getLockLastUpdatedTime()} is <5 min {@link MasterLockDto#getNodeName()}}
     *     currently has the lock. The lock kan be kept by the node withing this timespan, this will cause the
     *     {@link MasterLockDto#getLockLastUpdatedTime()} of the lock to update giving the node 5 more minutes
     *     to keep the lock</li>
     *     <li>If the lock {@link MasterLockDto#getLockLastUpdatedTime()} is >5 min and under <10 min no node currently
     *     has the lock. This is a limbo state where no node neither has it or can claim it.</li>
     *     <li>If the lock {@link MasterLockDto#getLockLastUpdatedTime()} is >10 min old then it is up for grabs by
     *     all nodes, the first node to claim it will then have the lock, it will try to keep the lock by
     *     updating the {@link MasterLockDto#getLockLastUpdatedTime()}</li>
     * <p>
     * The node that currently has the master lock is the node that has the responsibility to run the schedules.
     */
    Optional<MasterLockDto> getMasterLock();

    /**
     * Checks if the running node is currenly the master node.
     */
    boolean hasMasterLock();

    /**
     * Stops and closes all the schedules and the masterLocker. Used during shutdown of the running service.
     */
    void close();

    /**
     * The running schedule.
     */
    interface ScheduledTask {
        /**
         * Retrieve the name of the running schedule
         */
        String getScheduleName();

        /**
         * Sets this schedule to active meaning it will start executing the supplied runnable
         */
        void start();

        /**
         * Sets this schedule to inactive meaning it will still to the cronExpression schedules but skip the execution
         * of the supplied runnable.
         */
        void stop();

        /**
         * Sets a schedule to run immediately. Note it will first mark this schedule to run by setting a flag in the db,
         * then wake up the scheduler thread so it will be triggered, assuming this is called on the node that has
         * the master lock it will trigger nearly instantly. However if this where triggered by a node that does not
         * have the master lock it will delay for up to.
         * <p>
         * This will prepend a line to the logs informing this schedule run where manually started.
         * {@link ScheduledTaskServiceImpl#SLEEP_LOOP_MAX_SLEEP_AMOUNT_IN_MILLISECONDS} before it runs.
         */
        void runNow();

        /**
         * Check if this schedule is currently set to to active ie {@link #start()} (default on startup).
         * @return
         *          - True: It is executing the supplied runnable.
         *          - False: It is currently set to skip executing the supplied runnable. {@link #stop()}}
         *          has been used.
         */
        boolean isActive();

        /**
         * Retrun the last run done by the schedule
         */
        ScheduleRunContext getLastScheduleRun();

        /**
         * Retrieve all schedule runs between two dates. This filters by the start time of the schedule runs.
         */
        List<ScheduleRunContext> getAllScheduleRunsBetween(LocalDateTime from, LocalDateTime to);

        /**
         * Check if this current schedule is currently running. Can be used with {@link #isOverdue()} to check if this
         * schedule is taking longer than expected.
         */
        boolean isRunning();

        /**
         * Checks if this run is taking longer time than it where expected to use during creation of the schedule.
         */
        boolean isOverdue();

        /**
         * Set a new cronExpression to be used by the schedule. If this is set to null it will fallback to use the
         * schedule defined at {@link #getActiveCronExpression()}.
         */
        void setOverrideExpression(CronExpression newCronExpression);

        /**
         * Retrieve the cronExpression that where set when the schedule was created by
         * {@link #addSchedule(String, String, Axis, int, ScheduleRunnable)}.
         */
        CronExpression getDefaultCronExpression();

        /**
         * Get the current used cronExpression by this schedule. If it is not overridden by
         * {@link #setOverrideExpression(CronExpression)} then it will return the {@link #getDefaultCronExpression()}
         * <p>
         * If a new cronExpression has been set by using {@link #setOverrideExpression(CronExpression)} then that will
         * be used.
         */
        CronExpression getActiveCronExpression();

        /**
         * Retrieve the in-memory timestamp on when the last run where started. May be null if it has not yet
         * started a run.
         */
        Instant getLastRunStarted();

        /**
         * Retrieve the in-memory timestamp on when the last run where completed. Note this may be before the
         * {@link #getLastRunStarted()}, if it is then it means the schedule is currently running and has
         * not yet completed. May be null if it has not yet completed a run.
         */
        Instant getLastRunCompleted();

        /**
         * Retrieve the max amount of minutes this schedule is expected to run.
         */
        int getMaxExpectedMinutesToRun();

        /**
         * Retrieves the in memory instant on when this schedule is expected to run next.
         */
        Instant getNextRun();

        /**
         * Retrieve a specific {@link ScheduleRunContext}
         */
        ScheduleRunContext getInstance(String instanceId);
    }

    @FunctionalInterface
    interface ScheduleRunnable {
        ScheduleStatus run(ScheduleRunContext ctx);
    }


    /**
     * The historic runs of a given {@link ScheduledTask}
     */
    interface ScheduleRunContext {
        /**
         * Schedule name of this historic run
         */
        String getScheduledName();

        /**
         * Unique id identifying this historic run
         */
        String instanceId();

        /**
         * Retrieve the {@link ScheduledTask} responsible for this run.
         */
        ScheduledTask getSchedule();

        /**
         * Get the last historic run.
         */
        Instant getPreviousRun();

        /**
         * Get the last set {@link State}
         */
        State getStatus();

        /**
         * Get the last Status message. May be null if no status is yet set.
         */
        String getStatusMsg();

        /**
         * Get the status {@link Throwable}. Will be null if it where not added to the last status message set.
         */
        String getStatusThrowable();

        /**
         * Get the {@link LocalDateTime} on when this run where started.
         */
        LocalDateTime getRunStarted();

        /**
         * Get the {@link LocalDateTime} on when the last time the {@link State} where updated.
         */
        LocalDateTime getStatusTime();

        /**
         * Get all {@link LogEntry}.
         * This will be retrieved from the database since the logs is not kept in memory.
         */
        List<LogEntry> getLogEntries();

        /**
         * Add a log message to this historic run.
         * Can be called multiple times as lon as the {@link #failed(String)} or {@link #done(String)} is not set.
         */
        void log(String msg);

        /**
         * Add a log message with a throwable to this historic run.
         * Can be called multiple times as long as the {@link #failed(String)} or {@link #done(String)} is not set.
         */
        void log(String msg, Throwable throwable);

        /**
         * Sets this historic run to done. Meaning it is now completed and where successful. After this is set
         * it is no longer possible to set {@link #log(String)}, {@link #failed(String)}, {@link #done(String)} or
         * {@link #dispatched(String)}
         */
        ScheduleStatus done(String msg);

        /**
         * Sets this historic run to failed. This is will also set this run to 'completed but failed' meaning after this
         * is set it is no longer possible to set {@link #log(String)}, {@link #failed(String)},
         * {@link #done(String)} or {@link #dispatched(String)}
         */
        ScheduleStatus failed(String msg);
        /**
         * Sets this historic run to failed. This is will also set this run to 'completed but failed' meaning after this
         * is set it is no longer possible to set {@link #log(String)}, {@link #failed(String)},
         * {@link #done(String)} or {@link #dispatched(String)}
         */
        ScheduleStatus failed(String msg, Throwable throwable);

        /**
         * Sets this historic run to dispatched. Can be used with Mats<sup>3</sup> to notify this is now delegated to further
         * processing. Can be called multiple times in a row as long as the {@link #failed(String)}
         * or {@link #done(String)} is not set.
         */
        ScheduleStatus dispatched(String msg);
    }

    /**
     * You are not meant to implement this infertface but return a instance you get from {@link
     * ScheduleRunContext#done(String)}, {@link ScheduleRunContext#failed(String)} or {@link
     * ScheduleRunContext#dispatched(String)}
     */
    interface ScheduleStatus {

    }

    /**
     * Each log entry that is created with {@link ScheduleRunContext#log(String)}
     * and {@link ScheduleRunContext#log(String, Throwable)}
     */
    interface LogEntry {
        String getMessage();
        Optional<String> getThrowable();
        LocalDateTime getLogTime();
    }
}
