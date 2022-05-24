package com.storebrand.scheduledtask.db;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.storebrand.scheduledtask.ScheduledTaskRegistry.LogEntry;
import com.storebrand.scheduledtask.ScheduledTask.RetentionPolicy;
import com.storebrand.scheduledtask.ScheduledTaskRegistry.Schedule;
import com.storebrand.scheduledtask.ScheduledTaskRegistry.State;
import com.storebrand.scheduledtask.ScheduledTaskRegistryImpl;

/**
 * Interface for creating and storing scheduled tasks and run details in the database. An implementation is required for
 * scheduled tasks to work. This is meant for internal use inside {@link ScheduledTaskRegistryImpl}, and should not be
 * used outside this package, except for providing implementations.
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
    Map<String, Schedule> getSchedules();

    /**
     * Get the schedule with a specific name.
     */
    Optional<Schedule> getSchedule(String scheduleName);

    /**
     * Add a schedule run result to the db.
     *
     * @param scheduleName
     *         - Name of the schedule that did the run
     * @param hostname
     *         - The host that this run is running on.
     * @param runStart
     *         - When this run where started.
     * @param statusMsg
     *         - Short describing text informing that this scheduleRun is started.
     *         <p>
     *         Will by default set the state to {@link State#STARTED}
     * @return the run id of the new scheduled run.
     */
    long addScheduleRun(String scheduleName, String hostname, Instant runStart, String statusMsg);

    /**
     * Update the {@link State} of a schedule run.
     *
     * @param runId
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
    boolean setStatus(long runId, State state, String statusMsg, String statusStackTrace, Instant statusTime);

    boolean setStatus(ScheduledRunDto scheduledRunDto);

    /**
     * Get the specific {@link ScheduledRunDto} with the specified instanceId. Note this does not load the logs of the
     * schedule run. Use {@link #getLogEntries(long)} to fetch these.
     *
     * @param runId
     *         - The runId to retrieve the scheduled run for.
     * @return
     */
    Optional<ScheduledRunDto> getScheduleRun(long runId);

    /**
     * Get the last inserted ScheduleRun for the given scheduleName.
     * <p>
     * Note this does not load the logs of the schedule run. Use {@link #getLogEntries(long)} to fetch these.
     *
     * @param scheduleName
     *         - Name of the schedule to retrieve the last run from
     */
    Optional<ScheduledRunDto> getLastRunForSchedule(String scheduleName);

    /**
     * Retrieves all the last {@link ScheduledRunDto} for all schedule names. The results are sorted by {@link
     * ScheduledRunDto#getRunStart()} descending.
     * <p>
     * Note this does not load the logs of the schedule run. Use {@link #getLogEntries(long)} to fetch these.
     */
    List<ScheduledRunDto> getLastScheduleRuns();

    /**
     * Get all {@link ScheduledRunDto} between a given timespan. Note this does not load the logs of the schedule run.
     * Use {@link #getLogEntries(long)} to fetch these.
     *
     * @param from
     *         - from time and including
     * @param to
     *         - to and including
     */
    List<ScheduledRunDto> getScheduleRunsBetween(String scheduleName, LocalDateTime from, LocalDateTime to);

    /**
     * Add a log entry to a specified scheduled task run, by using the scheduled runs instanceId.
     * @param runId
     *         - ID for the schedule run to add the logs to.
     * @param logTime
     *         - log time.
     * @param message
     *         - log message.
     */
    default void addLogEntry(long runId, LocalDateTime logTime, String message) {
        addLogEntry(runId, logTime, message, null);
    }

    /**
     * Add a log entry to a specified scheduled task run, by using the scheduled runs instanceId.
     * @param runId
     *         - runId for the schedule run to add the logs to.
     * @param logTime
     *         - log time.
     * @param message
     *         - log message.
     * @param stackTrace
     *         - optional stack trace for error messages.
     */
    void addLogEntry(long runId, LocalDateTime logTime, String message, String stackTrace);

    /**
     * Get all logEntries for a given schedule run instance.
     *
     * @param runId
     *         - runId for the schedule run to add the logs to
     * @return List<LogEntryDbo> - The logEntries (if any) for that schedule run instance
     */
    List<LogEntry> getLogEntries(long runId);

    void executeRetentionPolicy(String scheduleName, RetentionPolicy retentionPolicy);

    // ===== DTOs ======================================================================================================

    /**
     * Holds the information on a current or previous run.
     */
    class ScheduledRunDto {
        private long runId;
        private final String scheduleName;
        private final String hostname;
        private State status;
        private String statusMsg;
        private String statusStackTrace;
        private final Instant runStart;
        private Instant statusTime;

        public ScheduledRunDto(long runId, String scheduleName, String hostname, State status, String statusMsg,
                String statusStackTrace, Instant runStart, Instant statusTime) {
            this.runId = runId;
            this.scheduleName = scheduleName;
            this.hostname = hostname;
            this.status = status;
            this.statusMsg = statusMsg;
            this.statusStackTrace = statusStackTrace;
            this.runStart = runStart;
            this.statusTime = statusTime;
        }

        public static ScheduledRunDto newWithStateStarted(long runId, String scheduleName, String hostname,
                Instant runStart) {
            return new ScheduledRunDto(runId, scheduleName, hostname, State.STARTED, null,
                    null, runStart, null);
        }

        public long getRunId() {
            return runId;
        }

        public String getScheduleName() {
            return scheduleName;
        }

        public String getHostname() {
            return hostname;
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
