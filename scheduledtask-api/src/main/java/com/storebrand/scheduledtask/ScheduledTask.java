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

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.storebrand.scheduledtask.ScheduledTaskRegistry.LogEntry;
import com.storebrand.scheduledtask.ScheduledTaskRegistry.RunOnce;
import com.storebrand.scheduledtask.ScheduledTaskRegistry.Schedule;
import com.storebrand.scheduledtask.ScheduledTaskRegistry.ScheduleRunContext;
import com.storebrand.scheduledtask.ScheduledTaskRegistry.ScheduleRunnable;

/**
 * Represents a running scheduled task.
 *
 * @author Dag Bertelsen - dag.lennart.bertelsen@storebrand.no - dabe@dagbertelsen.com - 2021.03
 * @author Kristian Hiim
 */
public interface ScheduledTask {
    /**
     * Retrieve the name of the running schedule, which corresponds to {@link Schedule#getName()}.
     */
    String getName();

    /**
     * Get the criticality level of the running schedule.
     */
    Criticality getCriticality();

    /**
     * Get the recovery mode of the running schedule.
     */
    Recovery getRecovery();

    /**
     * Get the retention policy that is defined for this schedule.
     */
    RetentionPolicy getRetentionPolicy();

    /**
     * Sets this schedule to active meaning it will start executing the supplied runnable.
     */
    void start();

    /**
     * Sets this schedule to inactive meaning it will still to the cronExpression schedules but skip the execution of
     * the supplied runnable.
     */
    void stop();

    /**
     * Sets a schedule to run immediately. Note it will first mark this schedule to run by setting a flag in the db,
     * then wake up the scheduler thread so it will be triggered, assuming this is called on the node that has the
     * master lock it will trigger nearly instantly. However, if this where triggered by a node that does not have the
     * master lock it will delay for a short amount of time depending on the implementation. The current default
     * implementation will sleep for up to two minutes between checking for new tasks.
     * <p>
     * This will prepend a line to the logs informing this schedule run was {@link RunOnce#PROGRAMMATIC} started.
     */
    void runNow();


    /**
     * Sets a schedule to run immediately. Note it will first mark this schedule to run by setting a flag in the db,
     * then wake up the scheduler thread so it will be triggered, assuming this is called on the node that has the
     * master lock it will trigger nearly instantly. However, if this where triggered by a node that does not have the
     * master lock it will delay for a short amount of time depending on the implementation. The current default
     * implementation will sleep for up to two minutes between checking for new tasks.
     * <p>
     * This will prepend a line to the logs informing this schedule run was {@link RunOnce} started.
     */
    void runNow(RunOnce runOnce);

    /**
     * Check if the schedule task thread is alive. This should in theory always be true, but if the thread has been
     * stopped by some external means it will return false.
     */
    boolean isThreadAlive();

    /**
     * Check if this schedule is currently set to active ie {@link #start()} (default on startup).
     *
     * @return - True: It is executing the supplied runnable. - False: It is currently set to skip executing the
     * supplied runnable. {@link #stop()}} has been used.
     */
    boolean isActive();

    /**
     * Return the last run done by the schedule.
     */
    Optional<ScheduleRunContext> getLastScheduleRun();

    /**
     * Retrieve all schedule runs between two dates. This filters by the start time of the schedule runs.
     */
    List<ScheduleRunContext> getAllScheduleRunsBetween(LocalDateTime from, LocalDateTime to);

    /**
     * Retrieves all schedule run ids between two dates with all the logs.
     */
    Map<Long, List<LogEntry>> getLogEntriesByRunId(LocalDateTime from, LocalDateTime to);

    /**
     * Check if this current schedule is currently running. Can be used with {@link #isOverdue()} to check if this
     * schedule is taking longer than expected.
     */
    boolean isRunning();

    /**
     * Checks if this run has passed expected run time.
     */
    boolean hasPassedExpectedRunTime(Instant runStarted);

    /**
     * Checks if this run is taking longer time than it was expected to use during creation of the schedule.
     */
    boolean isOverdue();

    /**
     * Returns running time in minutes, if task is running.
     */
    Optional<Long> runTimeInMinutes();

    /**
     * Set a new cronExpression to be used by the schedule. If this is set to null it will fallback to use the schedule
     * defined at {@link #getActiveCronExpression()}.
     */
    void setOverrideExpression(String newCronExpression);

    /**
     * Retrieve the cronExpression that where set when the schedule was created by
     * {@link ScheduledTaskRegistry#buildScheduledTask(String, String, ScheduleRunnable)}.
     */
    String getDefaultCronExpression();

    /**
     * Get the current used cronExpression by this schedule. If it is not overridden by {@link
     * #setOverrideExpression(String)} then it will return the {@link #getDefaultCronExpression()}
     * <p>
     * If a new cronExpression has been set by using {@link #setOverrideExpression(String)} then that will be used.
     */
    String getActiveCronExpression();

    /**
     * Retrieve the in-memory timestamp on when the last run where started. May be null if it has not yet started a
     * run.
     */
    Instant getLastRunStarted();

    /**
     * Retrieve the in-memory timestamp on when the last run where completed. Note this may be before the {@link
     * #getLastRunStarted()}, if it is then it means the schedule is currently running and has not yet completed. May be
     * null if it has not yet completed a run.
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
    ScheduleRunContext getInstance(long runId);

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
     * Retention policy for a scheduled task.
     */
    interface RetentionPolicy {
        int getDeleteRunsAfterDays();

        int getDeleteSuccessfulRunsAfterDays();

        int getDeleteFailedRunsAfterDays();

        int getDeleteNoopRunsAfterDays();

        int getKeepMaxFailedRuns();

        int getKeepMaxSuccessfulRuns();

        int getKeepMaxNoopRuns();

        int getKeepMaxRuns();

        boolean isRetentionPolicyEnabled();
    }
}
