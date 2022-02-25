package com.storebrand.scheduledtask;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * ScheduledTask methods for the scheduler service.
 *
 * @author Dag Bertelsen - dag.lennart.bertelsen@storebrand.no - dabe@dagbertelsen.com - 2021.03
 * @author Kristian Hiim
 */
public interface ScheduledTaskService {

    /**
     * Create a new schedule that will run at the given cron expression.
     *
     * @param scheduleName
     *         - Name of the schedule
     * @param cronExpression
     *         - When this schedule should run.
     * @param maxExpectedMinutes
     *         - Amount of minutes this should run.
     * @param runnable
     *         - The runnable that this schedule should run.
     */
    ScheduledTask addSchedule(String scheduleName, String cronExpression, int maxExpectedMinutes, ScheduleRunnable runnable);

    /**
     * Create a new schedule. Configure additional parameters on the {@link ScheduledTaskInitializer} and finally call
     * {@link ScheduledTaskInitializer#start()} to actually initialize and start the new scheduled task.
     *
     * @param name
     *         - Name of the schedule
     * @param cronExpression
     *         - When this schedule should run.
     * @param runnable
     *         - The runnable that this schedule should run.
     * @return ScheduledTaskInitializer used to configure optional parameters, and start the scheduled task.
     */
    ScheduledTaskInitializer addScheduledTask(String name, String cronExpression, ScheduleRunnable runnable);

    /**
     * Get a schedule with a specific name.
     */
    ScheduledTask getSchedule(String name);

    /**
     * Get a copy of all the schedules that are registered in the system.
     */
    Map<String, ScheduledTask> getSchedules();

    /**
     * Gets all schedules that are persisted in the current database. These might be different from what is in memory,
     * as there might be multiple services manipulating the schedules, and we should be able to read the current status
     * from the database.
     */
    List<Schedule> getSchedulesFromRepository();

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
     * returns an old lock you need to check {@link MasterLock#getLockLastUpdatedTime()} by the following rules:
     * <ol>
     *     <li>If the lock {@link MasterLock#getLockLastUpdatedTime()} is <5 min {@link MasterLock#getNodeName()}}
     *     currently has the lock. The lock kan be kept by the node withing this timespan, this will cause the
     *     {@link MasterLock#getLockLastUpdatedTime()} of the lock to update giving the node 5 more minutes
     *     to keep the lock</li>
     *     <li>If the lock {@link MasterLock#getLockLastUpdatedTime()} is >5 min and under <10 min no node currently
     *     has the lock. This is a limbo state where no node neither has it or can claim it.</li>
     *     <li>If the lock {@link MasterLock#getLockLastUpdatedTime()} is >10 min old then it is up for grabs by
     *     all nodes, the first node to claim it will then have the lock, it will try to keep the lock by
     *     updating the {@link MasterLock#getLockLastUpdatedTime()}</li>
     * <p>
     * The node that currently has the master lock is the node that has the responsibility to run the schedules.
     */
    Optional<MasterLock> getMasterLock();

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
         * Get the criticality level of the running schedule
         */
        Criticality getCriticality();

        /**
         * Get the recovery mode of the running schedule.
         */
        Recovery getRecovery();


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
         * have the master lock it will delay for a short amount of time depending on the implementation. The current
         * default implementation will sleep for up to two minutes between checking for new tasks.
         * <p>
         * This will prepend a line to the logs informing this schedule run where manually started.
         */
        void runNow();

        /**
         * Check if this schedule is currently set to active ie {@link #start()} (default on startup).
         * @return
         *          - True: It is executing the supplied runnable.
         *          - False: It is currently set to skip executing the supplied runnable. {@link #stop()}}
         *          has been used.
         */
        boolean isActive();

        /**
         * Return the last run done by the schedule
         */
        Optional<ScheduleRunContext> getLastScheduleRun();

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
         * Returns running time in minutes, if task is running.
         */
        Optional<Long> runTimeInMinutes();

        /**
         * Set a new cronExpression to be used by the schedule. If this is set to null it will fallback to use the
         * schedule defined at {@link #getActiveCronExpression()}.
         */
        void setOverrideExpression(String newCronExpression);

        /**
         * Retrieve the cronExpression that where set when the schedule was created by
         * {@link #addSchedule(String, String, int, ScheduleRunnable)}.
         */
        String getDefaultCronExpression();

        /**
         * Get the current used cronExpression by this schedule. If it is not overridden by
         * {@link #setOverrideExpression(String)} then it will return the {@link #getDefaultCronExpression()}
         * <p>
         * If a new cronExpression has been set by using {@link #setOverrideExpression(String)} then that will
         * be used.
         */
        String getActiveCronExpression();

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

    /**
     * Represents the current state of a task.
     */
    enum State {
        STARTED,
        FAILED,
        DISPATCHED,
        DONE
    }

    /**
     * Criticality defines four levels that signals how important a scheduled task is, and can be used in monitoring systems
     * to determine how to show failed runs. What each criticality level represents is entirely up to the user, but an
     * example definition of the levels is provided for each. Paired with {@link Recovery} it helps prioritize what failed
     * tasks should be looked into.
     */
    enum Criticality {
        /**
         * These tasks are absolute critical to the function of a service. If it is not running as expected this will have a
         * great impact on the operation. Recovery time should typically be measured in terms of hours, not days.
         */
        MISSION_CRITICAL,
        /**
         * These are tasks that fall between mission critical and important tasks. They are not as critical as the most
         * critical tasks, but issues needs to be resolved as soon as possible after resolving any critical issues. Recovery
         * time can typically be measured in hours, or at most a day or two.
         */
        VITAL,
        /**
         * If these tasks fail it won't stop the service from functioning, but it is still an important task. If it does not
         * work the service should still be able to perform its primary function, but it might not be able to deliver all
         * functionality. Recovery time can be measured in days, or perhaps weeks.
         */
        IMPORTANT,
        /**
         * These are minor tasks, that are not critical to the service. If they are not running as they should the service
         * will have some minor issues that can easily be resolved.
         */
        MINOR
    }

    /**
     * Recovery defines if a scheduled task is able to fix itself, or if failed tasks must be handled manually by human
     * interaction.
     */
    enum Recovery {
        /**
         * Self-healing scheduled task will typically recover the next time they run, and it is probably not necessary
         * to take action unless the service keeps failing multiple times. Setting this means that the task should be
         * able to handle that the previous run(s) failed, and should pick up where it stopped on the last run.
         * <p>
         * Fixing a self-healing task that has failed should be as easy as triggering the task again, or simply waiting
         * for it to run again.
         */
        SELF_HEALING,
        /**
         * Manual intervention is used if a failed scheduled task requires manual cleanup, or will not recover from a
         * failed run by simply running the task again.
         */
        MANUAL_INTERVENTION
    }

    /**
     * Interface that all tasks are required to implement. Contains a run method that should perform the actual task.
     */
    @FunctionalInterface
    interface ScheduleRunnable {
        ScheduleStatus run(ScheduleRunContext ctx);
    }


    /**
     * The context for a specific run of a {@link ScheduledTask}.
     */
    interface ScheduleRunContext {
        /**
         * Schedule name of this run
         */
        String getScheduledName();

        /**
         * Unique id identifying this run
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
         * Get the status {@link Throwable}. Will be null if it was not added to the last status message set.
         */
        String getStatusStackTrace();

        /**
         * Get the {@link LocalDateTime} on when this run was started.
         */
        LocalDateTime getRunStarted();

        /**
         * Get the {@link LocalDateTime} on when the last time the {@link State} was updated.
         */
        LocalDateTime getStatusTime();

        /**
         * Get all {@link LogEntry}.
         * This will be retrieved from the database since the logs are not kept in memory.
         */
        List<LogEntry> getLogEntries();

        /**
         * Add a log message to this run.
         * Can be called multiple times as long as the {@link #failed(String)} or {@link #done(String)} is not set.
         */
        void log(String msg);

        /**
         * Add a log message with a throwable to this run.
         * Can be called multiple times as long as the {@link #failed(String)} or {@link #done(String)} is not set.
         */
        void log(String msg, Throwable throwable);

        /**
         * Sets this run to done. Meaning it is now completed and was successful. After this is set it is no longer
         * possible to call {@link #log(String)}, {@link #failed(String)}, {@link #done(String)} or
         * {@link #dispatched(String)}
         */
        ScheduleStatus done(String msg);

        /**
         * Sets this run to failed. This is will also set this run to 'completed but failed' meaning after this is set
         * it is no longer possible to call {@link #log(String)}, {@link #failed(String)}, {@link #done(String)} or
         * {@link #dispatched(String)}
         */
        ScheduleStatus failed(String msg);

        /**
         * Sets this run to failed. This is will also set this run to 'completed but failed' meaning after this is set
         * it is no longer possible to call {@link #log(String)}, {@link #failed(String)}, {@link #done(String)} or
         * {@link #dispatched(String)}
         */
        ScheduleStatus failed(String msg, Throwable throwable);

        /**
         * Sets this run to dispatched. Can be used with Mats<sup>3</sup> to notify this is now delegated to further
         * processing. Can be called multiple times in a row as long as the {@link #failed(String)} or
         * {@link #done(String)} is not set.
         */
        ScheduleStatus dispatched(String msg);
    }

    /**
     * You are not meant to implement this interface but return an instance you get from
     * {@link ScheduleRunContext#done(String)}, {@link ScheduleRunContext#failed(String)} or
     * {@link ScheduleRunContext#dispatched(String)}
     */
    interface ScheduleStatus {

    }

    /**
     * The current status of a schedule retrieved from the database.
     */
    interface Schedule {
        /**
         * The name of the schedule
         */
        String getScheduleName();

        /**
         * Informs if this schedule is currently active or not. IE is it currently set to execute the runnable part. It will
         * still "do the loop schedule" except it will skip running the supplied runnable if this is set to false.
         */
        boolean isActive();

        /**
         * If set to true infroms that this should run now regardless of the schedule, also it should only run now once. It
         * is used from the monitor when a user clicks the "run now" button, this will be written to the db where the master
         * node will pick it up and run it as soon as it checks the nextRun instant.
         */
        boolean isRunOnce();

        /**
         * If set informs that this schedule has a new cron expression that differs from the one defined in the code.
         */
        Optional<String> getOverriddenCronExpression();

        /**
         * The instance on when the schedule is set to run next.
         */
        Instant getNextRun();

        /**
         * When this schedule where last updated.
         */
        Instant getLastUpdated();
    }

    /**
     * Interface for a single log entry for a scheduled task.
     */
    interface LogEntry {
        /**
         * The instance ID that this log entry is attached to.
         */
        String getInstanceId();

        /**
         * The log message.
         */
        String getMessage();

        /**
         * Optional stack trace for error messages.
         */
        Optional<String> getStackTrace();

        /**
         * The time this log message was written.
         */
        LocalDateTime getLogTime();
    }
}
