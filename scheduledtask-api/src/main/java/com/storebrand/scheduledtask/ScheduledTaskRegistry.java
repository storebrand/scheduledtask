package com.storebrand.scheduledtask;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * ScheduledTask registry API. This is the main entry point for applications that use scheduled tasks.
 *
 * @author Dag Bertelsen - dag.lennart.bertelsen@storebrand.no - dabe@dagbertelsen.com - 2021.03
 * @author Kristian Hiim
 */
public interface ScheduledTaskRegistry {

    /**
     * Create a new schedule. Configure additional parameters on the {@link ScheduledTaskBuilder} and finally call
     * {@link ScheduledTaskBuilder#start()} to actually initialize and start the new scheduled task.
     *
     * @param name
     *         - Name of the schedule
     * @param cronExpression
     *         - When this schedule should run.
     * @param runnable
     *         - The runnable that this schedule should run.
     * @return ScheduledTaskBuilder used to configure optional parameters, and start the scheduled task.
     */
    ScheduledTaskBuilder buildScheduledTask(String name, String cronExpression, ScheduleRunnable runnable);

    /**
     * Adds a scheduled task, and immediately starts it. This will use default values for all optional parameters. If
     * you need to configure any parameters use {@link #buildScheduledTask(String, String, ScheduleRunnable)}, and call
     * {@link ScheduledTaskBuilder#start()} after defining additional parameters.
     *
     * @param name
     *         - Name of the schedule
     * @param cronExpression
     *         - When this schedule should run.
     * @param runnable
     *         - The runnable that this schedule should run.
     * @return a ScheduledTask implementation that is now registered in the service, and runs the schedule.
     */
    default ScheduledTask addAndStartScheduledTask(String name, String cronExpression, ScheduleRunnable runnable) {
        return buildScheduledTask(name, cronExpression, runnable).start();
    }

    /**
     * Get a schedule with a specific name.
     */
    ScheduledTask getScheduledTask(String name);

    /**
     * Get a copy of all the schedules that are registered in the system.
     */
    Map<String, ScheduledTask> getScheduledTasks();

    /**
     * Gets all schedules that are persisted in the current database. These might be different from what is in memory,
     * as there might be multiple services manipulating the schedules, and we should be able to read the current status
     * from the database.
     */
    Map<String, Schedule> getSchedulesFromRepository();

    /**
     * Get information if any node currently has the master lock and if so what node currently has it. There is also a
     * chance that the lock is not set to anyone and this return who had it last, to figure out if this returns an old
     * lock you need to check {@link MasterLock#getLockLastUpdatedTime()} by the following rules:
     * <ol>
     *     <li>If the lock {@link MasterLock#getLockLastUpdatedTime()} is &lt;5 min {@link MasterLock#getNodeName()}}
     *     currently has the lock. The lock kan be kept by the node withing this timespan, this will cause the
     *     {@link MasterLock#getLockLastUpdatedTime()} of the lock to update giving the node 5 more minutes
     *     to keep the lock</li>
     *     <li>If the lock {@link MasterLock#getLockLastUpdatedTime()} is &gt;5 min and under &lt;10 min no node currently
     *     has the lock. This is a limbo state where no node neither has it or can claim it.</li>
     *     <li>If the lock {@link MasterLock#getLockLastUpdatedTime()} is &gt;10 min old then it is up for grabs by
     *     all nodes, the first node to claim it will then have the lock, it will try to keep the lock by
     *     updating the {@link MasterLock#getLockLastUpdatedTime()}</li>
     * <p>
     * The node that currently has the master lock is the node that has the responsibility to run the schedules.
     */
    Optional<MasterLock> getMasterLock();

    /**
     * Checks if the running node is currently the master node.
     */
    boolean hasMasterLock();

    /**
     * Stops and closes all the schedules and the masterLocker. Used during shutdown of the running service.
     */
    void close();

    /**
     * Add a listener that will be notified when scheduled tasks are created. Can potentially be extended in the future
     * to cover more events.
     */
    void addListener(ScheduledTaskListener listener);

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
         * Get all {@link LogEntry}. This will be retrieved from the database since the logs are not kept in memory.
         */
        List<LogEntry> getLogEntries();

        /**
         * Add a log message to this run. Can be called multiple times as long as the {@link #failed(String)} or
         * {@link #done(String)} is not set.
         */
        void log(String msg);

        /**
         * Add a log message with a throwable to this run. Can be called multiple times as long as the
         * {@link #failed(String)} or {@link #done(String)} is not set.
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
         * The name of the schedule, which corresponds with the {@link ScheduledTask#getName()}.
         */
        String getName();

        /**
         * Informs if this schedule is currently active or not. IE is it currently set to execute the runnable part. It
         * will still "do the loop schedule" except it will skip running the supplied runnable if this is set to false.
         */
        boolean isActive();

        /**
         * If set to true informs that this should run now regardless of the schedule, also it should only run now once.
         * It is used from the monitor when a user clicks the "run now" button, this will be written to the db where the
         * master node will pick it up and run it as soon as it checks the nextRun instant.
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
         * When this schedule was last updated.
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

    /**
     * Information about the current master lock for scheduled tasks.
     */
    interface MasterLock {
        String getLockName();

        String getNodeName();

        Instant getLockTakenTime();

        Instant getLockLastUpdatedTime();

        /**
         * Check if this lock is still valid. If it is over 5 min old it is invalid meaning this host where the one to
         * have it last. The lock can't be re-claimed before it has passed 10 min since last update.
         */
        boolean isValid(Instant now);
    }

    /**
     * Listener that can be used to detect events regarding scheduled tasks. Only supports notifying that a {@link
     * ScheduledTask} has been created at the moment.
     */
    interface ScheduledTaskListener {
        /**
         * A scheduled task has been created.
         */
        void onScheduledTaskCreated(ScheduledTask scheduledTask);
    }
}
