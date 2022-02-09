package com.storebrand.scheduledtask.db;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Optional;

import com.storebrand.scheduledtask.ScheduledTaskService.LogEntry;
import com.storebrand.scheduledtask.ScheduledTaskService.State;

/**
 * Interface for creating and storing scheduled tasks and run details in the database. This is meant for internal use
 * inside the implementation of {@link com.storebrand.scheduledtask.ScheduledTaskService}.
 *
 * @author Dag Bertelsen - dag.lennart.bertelsen@storebrand.no - dabe@dagbertelsen.com - 2021.02
 * @author Kristian Hiim
 */
public interface ScheduledTaskRepository {
    /**
     * If the specified schedule is missing in the database it will create it. By default, all new schedules will be set
     * to <b>active</b>
     *
     * @param scheduleName
     *         - Name of the schedule.
     * @param cronExpression
     *         - Cron expression to insert, this can be null. Only added if we want to override the default expression.
     * @param nextRun
     *         - When the next run should be triggered.
     * @return int - Amount of inserted rows. 0 or 1.
     */
    int createSchedule(String scheduleName, String cronExpression, Instant nextRun);

    /**
     * If the specified schedule is missing in the database it will create it. By default, all new schedules will be set
     * to <b>active</b>
     *
     * @param scheduleName
     *         - Name of the schedule.
     * @param nextRun
     *         - When the next run should be triggered.
     * @return int - Amount of inserted rows. 0 or 1.
     */
    default int createSchedule(String scheduleName, Instant nextRun) {
        return createSchedule(scheduleName, null, nextRun);
    }

    /**
     * Update the next run for a given schedule.
     *
     * @param scheduleName
     *         - Name of the schedule
     * @param overrideCronExpression
     *         - if set defines the overridden cron expression that is used to calculate the nextRun
     * @param nextRun
     *         - When the schedule should run next
     * @return int - amount of updates done
     */
    int updateNextRun(String scheduleName, String overrideCronExpression, Instant nextRun);

    /**
     * Set the active state on a given schedule. If this is set to false then the scheduler will do the "looping" bit
     * but skip the actual execution of the schedule it self.
     *
     * @param scheduleName
     *         - name of the schedule
     * @param active
     *         - true if the schedule should execute the work during the run.
     * @return int - amount of updates
     */
    int setActive(String scheduleName, boolean active);

    int setRunOnce(String scheduleName, boolean runOnce);

    /**
     * Get all schedules in the database
     */
    List<ScheduleDto> getSchedules();

    /**
     * Get the schedule with a specific name.
     */
    Optional<ScheduleDto> getSchedule(String scheduleName);

    /**
     * Add a schedule run result to the db.
     *
     * @param scheduleName
     *         - Name of the schedule that did the run
     * @param instanceId
     *         - Unique identifier for this run.
     * @param runStart
     *         - When this run where started.
     * @param statusMsg
     *         - Short describing text informing that this scheduleRun is started.
     *         <p>
     *         Will by default set the state to {@link State#STARTED}
     * @return boolean - true if the run where inserted.
     */
    boolean addScheduleRun(String scheduleName, String instanceId, Instant runStart, String statusMsg);

    /**
     * Update the {@link State} of a schedule run.
     *
     * @param instanceId
     *         - Unique id for the schedule run to update.
     * @param state
     *         - a new {@link State} to set for this run. NOTE the {@link State#STARTED} is not valid here. Also the
     *         states {@link State#FAILED} and {@link State#DONE} can only be set once.
     * @param statusMsg
     *         - A describing text informing about the state change.
     * @param statusStackTrace
     *         - Optional, a stack trace set when the state change is {@link State#FAILED}
     * @return boolean, true if the update where successful.
     */
    boolean setStatus(String instanceId, State state, String statusMsg, String statusStackTrace, Instant statusTime);

    boolean setStatus(ScheduledRunDto scheduledRunDto);

    /**
     * Get the specific {@link ScheduledRunDto} with the specified instanceId. Note this does not load the logs of the
     * schedule run. Use {@link #getLogEntries(String)} to fetch these.
     *
     * @param instanceId
     *         - The instanceId to retrieve the scheduled run for.
     * @return
     */
    Optional<ScheduledRunDto> getScheduleRun(String instanceId);

    /**
     * Get the last inserted ScheduleRun for the given scheduleName.
     * <p>
     * Note this does not load the logs of the schedule run. Use {@link #getLogEntries(String)} to fetch these.
     *
     * @param scheduleName
     *         - Name of the schedule to retrieve the last run from
     */
    Optional<ScheduledRunDto> getLastRunForSchedule(String scheduleName);

    /**
     * Retrieves all the last {@link ScheduledRunDto} for all schedule names. The results are sorted by {@link
     * ScheduledRunDto#getRunStart()} descending.
     * <p>
     * Note this does not load the logs of the schedule run. Use {@link #getLogEntries(String)} to fetch these.
     */
    List<ScheduledRunDto> getLastScheduleRuns();

    /**
     * Get all {@link ScheduledRunDto} between a given timespan. Note this does not load the logs of the schedule run.
     * Use {@link #getLogEntries(String)} to fetch these.
     *
     * @param from
     *         - from time and including
     * @param to
     *         - to and including
     */
    List<ScheduledRunDto> getScheduleRunsBetween(String scheduleName, LocalDateTime from, LocalDateTime to);

    /**
     * Add a {@link LogEntry} to a specified ScheduleRun by using the scheduleRun's instanceId.
     *
     * @param instanceId
     *         - InstanceId for the schedule run to add the logs to
     * @param logEntry
     *         - A {@link LogEntry} to insert.
     * @return int - amount of inserts.
     */
    int addLogEntry(String instanceId, LogEntry logEntry);

    /**
     * Get all logEntries for a given schedule run instance.
     *
     * @param instanceId
     *         - InstanceId for the schedule run to add the logs to
     * @return List<LogEntryDbo> - The logEntries (if any) for that schedule run instance
     */
    List<LogEntry> getLogEntries(String instanceId);

    // ===== DTOs ======================================================================================================

    /**
     * The schedule settings retrieved from the database.
     */
    class ScheduleDto {
        private final String scheduleName;
        private final boolean active;
        private final boolean runOnce;
        private final String overriddenCronExpression;
        private final Instant nextRun;
        private final Instant lastUpdated;

        public ScheduleDto(String scheduleName, boolean active, boolean runOnce, String cronExpression,
                Instant nextRun, Instant lastUpdated) {
            this.scheduleName = scheduleName;
            this.active = active;
            this.runOnce = runOnce;
            this.overriddenCronExpression = cronExpression;
            this.nextRun = nextRun;
            this.lastUpdated = lastUpdated;
        }

        /**
         * The name of the schedule
         */
        public String getScheduleName() {
            return scheduleName;
        }

        /**
         * Informs if this schedule is currently active or not. IE is it currently set to execute the runnable part.
         * It will still "do the loop schedule" except it will skip running the supplied runnable if this is set
         * to false.
         */
        public boolean isActive() {
            return active;
        }

        /**
         * If set to true infroms that this should run now regardless of the schedule, also it should only run now once.
         * It is used from the monitor when a user clicks the "run now" button, this will be written to the db where the
         * master node will pick it up and run it as soon as it checks the nextRun instant.
         */
        public boolean isRunOnce() {
            return runOnce;
        }

        /**
         * If set informs that this schedule has a new cron expression that differs from the one defined in the code.
         */
        public Optional<String> getOverriddenCronExpression() {
            return Optional.ofNullable(overriddenCronExpression);
        }

        /**
         * The instance on when the schedule is set to run next.
         */
        public Instant getNextRun() {
            return nextRun;
        }

        /**
         * When this schedule where last updated.
         */
        public Instant getLastUpdated() {
            return lastUpdated;
        }
    }

    /**
     * Holds the information on a current or previous run.
     */
    class ScheduledRunDto {
        private final String scheduleName;
        private final String instanceId;
        private State status;
        private String statusMsg;
        private String statusStackTrace;
        private final Instant runStart;
        private Instant statusTime;

        public ScheduledRunDto(String scheduleName, String instanceId, State status, String statusMsg,
                String statusStackTrace, Instant runStart, Instant statusTime) {
            this.scheduleName = scheduleName;
            this.instanceId = instanceId;
            this.status = status;
            this.statusMsg = statusMsg;
            this.statusStackTrace = statusStackTrace;
            this.runStart = runStart;
            this.statusTime = statusTime;
        }

        public static ScheduledRunDto newWithStateStarted(String scheduleName, String instanceId, Instant runStart) {
            return new ScheduledRunDto(scheduleName, instanceId, State.STARTED, null,
                    null, runStart, null);
        }

        public String getScheduleName() {
            return scheduleName;
        }

        public String getInstanceId() {
            return instanceId;
        }

        public State getStatus() {
            return status;
        }

        public String getStatusMsg() {
            return statusMsg;
        }

        public String getStatusStackTrace() {
            return statusStackTrace;
        }

        public LocalDateTime getRunStart() {
            return LocalDateTime.ofInstant(runStart, ZoneId.systemDefault());
        }

        public Instant getStatusInstant() {
            return statusTime;
        }

        public LocalDateTime getStatusTime() {
            return LocalDateTime.ofInstant(statusTime, ZoneId.systemDefault());
        }

        public void setStatus(State status, Instant statusTime, String statusMsg) {
            this.status = status;
            this.statusTime = statusTime;
            this.statusMsg = statusMsg;
            this.statusStackTrace = null;
        }

        public void setStatus(State status, Instant statusTime, String statusMsg, String stackTrace) {
            this.status = status;
            this.statusTime = statusTime;
            this.statusMsg = statusMsg;
            this.statusStackTrace = stackTrace;
        }
    }
}
