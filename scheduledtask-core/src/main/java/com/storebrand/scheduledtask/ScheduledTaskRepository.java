package com.storebrand.scheduledtask;

import static java.util.stream.Collectors.toList;

import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.storebrand.scheduledtask.ScheduledTaskService.LogEntry;
import com.storebrand.scheduledtask.ScheduledTaskServiceImpl.LogEntryImpl;
import com.storebrand.scheduledtask.ScheduledTaskServiceImpl.ScheduleDto;
import com.storebrand.scheduledtask.ScheduledTaskServiceImpl.ScheduledRunDto;
import com.storebrand.scheduledtask.ScheduledTaskServiceImpl.State;
import com.storebrand.scheduledtask.TableInspector.TableValidationException;
import com.storebrand.scheduledtask.internal.cron.CronExpression;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Handles storing and updating of the {@link ScheduledTaskServiceImpl} schedules and run logs.
 *
 * @author Dag Bertelsen - dag.lennart.bertelsen@storebrand.no - dabe@dagbertelsen.com - 2021.02
 */
public class ScheduledTaskRepository {
    private static final Logger log = LoggerFactory.getLogger(ScheduledTaskRepository.class);
    public static final String SCHEDULE_TASK_TABLE = "stb_schedule";
    public static final String SCHEDULE_RUN_TABLE = "stb_schedule_run";
    public static final String SCHEDULE_LOG_ENTRY_TABLE = "stb_schedule_log_entry";
    private final DataSource _dataSource;
    private final Clock _clock;

    public ScheduledTaskRepository(DataSource dataSource, Clock clock) {
        _dataSource = dataSource;
        _clock = clock;

        validateScheduledTaskTableStructure();
        validateScheduleRunTableStructure();
        validateScheduleLogEntryTableStructure();
    }

    /**
     * If the specified schedule is missing in the database it will create it. By default all new schedules will be set
     * to <b>active</b>
     *
     * @param scheduleName
     *         - Name of the schedule.
     * @param cronExpression
     *         - Cronexpression to insert, this can be null. Only added if it should be overridden by default.
     * @param nextRun
     *         - When the next run should be triggered.
     * @return int - Amount of inserted rows. 0 or 1.
     */
    @SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    int createSchedule(String scheduleName, CronExpression cronExpression, Instant nextRun) {
        String sql = "INSERT INTO " + SCHEDULE_TASK_TABLE
                + " (schedule_name, is_active, run_once, cron_expression, next_run, last_updated) "
                + " SELECT ?, ?, ?, ?, ?, ? "
                + " WHERE NOT EXISTS (SELECT schedule_name FROM " + SCHEDULE_TASK_TABLE
                + " WHERE schedule_name = ?)";

        log.debug("Trying to create schedule [" + scheduleName + "]");

        try (Connection sqlConnection = _dataSource.getConnection();
             PreparedStatement pStmt = sqlConnection.prepareStatement(sql)) {
            String cron = cronExpression == null ? null : cronExpression.toString();
            pStmt.setString(1, scheduleName);
            // All schedules when created is by default active
            pStmt.setBoolean(2, true);
            // All new schedules should not have run once set
            pStmt.setBoolean(3, false);
            pStmt.setString(4, cron);
            pStmt.setTimestamp(5, Timestamp.from(nextRun));
            pStmt.setTimestamp(6, Timestamp.from(_clock.instant()));
            pStmt.setString(7, scheduleName);
            return pStmt.executeUpdate();
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * If the specified schedule is missing in the database it will create it. By default all new schedules will be set
     * to <b>active</b>
     *
     * @param scheduleName
     *         - Name of the schedule.
     * @param nextRun
     *         - When the next run should be triggered.
     * @return int - Amount of inserted rows. 0 or 1.
     */
    int createSchedule(String scheduleName, Instant nextRun) {
        return createSchedule(scheduleName, null, nextRun);
    }

    /**
     * Update the next run for a given schedule.
     *
     * @param scheduleName
     *         - Name of the schedule
     * @param overrideCron
     *         - if set defines the overriden {@link CronExpression} that is used to calculate the nextRun
     * @param nextRun
     *         - When the schedule should run next
     * @return int - amount of updates done
     */
    @SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    public int updateNextRun(String scheduleName, CronExpression overrideCron, Instant nextRun) {
        // :? Check if we should insert the schedule first:
        if (createSchedule(scheduleName, overrideCron, nextRun) == 1) {
            // -> Yes, we managed to insert the schedule so no need of doing an update.
            return 1;
        }

        // E-> Schedule already exists so we need to update it.
        String sql = "UPDATE " + SCHEDULE_TASK_TABLE
                + " SET schedule_name = ?, cron_expression = ?, next_run = ?, last_updated = ? "
                + " WHERE schedule_name = ?";

        log.info("Updating next run to [" + scheduleName + "] cronExpression [" + overrideCron + "] "
                + "and nextRun [" + nextRun + "]");

        try (Connection sqlConnection = _dataSource.getConnection();
             PreparedStatement pStmt = sqlConnection.prepareStatement(sql)) {
            String cron = overrideCron == null ? null : overrideCron.toString();
            pStmt.setString(1, scheduleName);
            pStmt.setString(2, cron);
            pStmt.setTimestamp(3, Timestamp.from(nextRun));
            pStmt.setTimestamp(4, Timestamp.from(_clock.instant()));
            pStmt.setString(5, scheduleName);
            return pStmt.executeUpdate();
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

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
    @SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    public int setActive(String scheduleName, boolean active) {
        String sql = "UPDATE " + SCHEDULE_TASK_TABLE
                + " SET schedule_name = ?, is_active = ? "
                + " WHERE schedule_name = ?";

        log.info("Schedule [" + scheduleName + "] is now set to be isActive [" + active + "] ");

        try (Connection sqlConnection = _dataSource.getConnection();
             PreparedStatement pStmt = sqlConnection.prepareStatement(sql)) {
            pStmt.setString(1, scheduleName);
            pStmt.setBoolean(2, active);
            pStmt.setString(3, scheduleName);
            return pStmt.executeUpdate();
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    public int setRunOnce(String scheduleName, boolean runOnce) {
        String sql = "UPDATE " + SCHEDULE_TASK_TABLE
                + " SET schedule_name = ?, run_once = ? "
                + " WHERE schedule_name = ?";

        log.info("Schedule [" + scheduleName + "] is now set to be isRunOnce [" + runOnce + "] ");

        try (Connection sqlConnection = _dataSource.getConnection();
             PreparedStatement pStmt = sqlConnection.prepareStatement(sql)) {
            pStmt.setString(1, scheduleName);
            pStmt.setBoolean(2, runOnce);
            pStmt.setString(3, scheduleName);
            return pStmt.executeUpdate();
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Get all schedules in the database
     */
    @SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    public List<ScheduleDto> getSchedules() {
        String sql = "SELECT * FROM " + SCHEDULE_TASK_TABLE;

        try (Connection sqlConnection = _dataSource.getConnection();
             PreparedStatement pStmt = sqlConnection.prepareStatement(sql);
             ResultSet result = pStmt.executeQuery()) {

            List<ScheduleDbo> schedules = new ArrayList<>();
            while (result.next()) {
                ScheduleDbo row = new ScheduleDbo(
                        result.getString("schedule_name"),
                        result.getBoolean("is_active"),
                        result.getBoolean("run_once"),
                        result.getString("cron_expression"),
                        result.getTimestamp("next_run"),
                        result.getTimestamp("last_updated"));
                schedules.add(row);
            }
            return schedules.stream()
                    .map(scheduleDbo -> ScheduleDto.fromDbo(scheduleDbo))
                    .collect(toList());
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Get the schedule with a specific name.
     */
    @SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    public Optional<ScheduleDto> getSchedule(String scheduleName) {
        String sql = "SELECT * FROM " + SCHEDULE_TASK_TABLE
                + " WHERE schedule_name = ? ";

        try (Connection sqlConnection = _dataSource.getConnection();
             PreparedStatement pStmt = sqlConnection.prepareStatement(sql)) {
            pStmt.setString(1, scheduleName);
            try (ResultSet result = pStmt.executeQuery()) {
                // ?: Did we find any row?
                if (result.first()) {
                    // -> Yes we found the first row
                    ScheduleDbo scheduleDbo = new ScheduleDbo(
                            result.getString("schedule_name"),
                            result.getBoolean("is_active"),
                            result.getBoolean("run_once"),
                            result.getString("cron_expression"),
                            result.getTimestamp("next_run"),
                            result.getTimestamp("last_updated"));
                    return Optional.of(ScheduleDto.fromDbo(scheduleDbo));
                }

                // E-> No, we did not find anything
                return Optional.empty();
            }
        }
        catch (SQLException throwables) {
            throw new RuntimeException(throwables);
        }
    }

    /**
     * Add a schedule run result to the db.
     *
     * @param scheduleName
     *         - Name of the schedule that did the run
     * @param instanceId
     *         - Unique identifier for this run.
     * @param runStart
     *         - When this run where started.
     * @param statusMsg - Short describing text informing that this scheduleRun is started.
     * <p>
     * Will by default set the state to {@link State#STARTED}
     *
     * @return boolean - true if the run where inserted.
     */
    @SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    public boolean addScheduleRun(String scheduleName, String instanceId, Instant runStart, String statusMsg) {
        String sql = "INSERT INTO " + SCHEDULE_RUN_TABLE
                + " (instance_id, schedule_name, status, status_msg, run_start, status_time) "
                + " SELECT ?, ?, ?, ?, ?, ? "
                + " WHERE NOT EXISTS (SELECT instance_id FROM " + SCHEDULE_RUN_TABLE
                + " WHERE instance_id = ?)";

        log.debug("Adding scheduleRun for scheuleName [" + scheduleName + "], instanceId [" + instanceId + "]");

        try (Connection sqlConnection = _dataSource.getConnection();
             PreparedStatement pStmt = sqlConnection.prepareStatement(sql)) {
            pStmt.setString(1, instanceId);
            pStmt.setString(2, scheduleName);
            pStmt.setString(3, State.STARTED.toString());
            pStmt.setString(4, statusMsg);
            pStmt.setTimestamp(5, Timestamp.from(runStart));
            pStmt.setTimestamp(6, Timestamp.from(_clock.instant()));
            pStmt.setString(7, instanceId);
            return pStmt.executeUpdate() == 1;
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Update the {@link State} of a schedule run.
     *
     * @param instanceId
     *         - Unique id for the schedule run to update.
     * @param state
     *         - a new {@link State} to set for this run. NOTE the {@link State#STARTED} is not valid here.
     *         Also the states {@link State#FAILED} and {@link State#DONE} can only be set once.
     * @param statusMsg
     *         - A describing text informing about the state change.
     * @param statusThrowable
     *         - Optional, a throwable set when the state change is {@link State#FAILED}
     *
     * @return boolean, true if the update where successful.
     */
    @SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    public boolean setStatus(String instanceId, State state, String statusMsg, String statusThrowable, Instant statusTime) {
        String sql = "UPDATE " + SCHEDULE_RUN_TABLE
                + " SET status = ?, status_msg = ?, status_throwable = ?, status_time = ? "
                + " WHERE instance_id = ?";

        if (state.equals(State.STARTED)) {
            throw new IllegalArgumentException("The state STARTED can only be set during the addScheduleRun");
        }

        try (Connection sqlConnection = _dataSource.getConnection();
             PreparedStatement pStmt = sqlConnection.prepareStatement(sql)) {

            // We should only allow to acquire the lock if the last_updated_time is older than 10 minutes.
            // Then it means it is up for grabs.
            pStmt.setString(1, state.toString());
            pStmt.setString(2, statusMsg);
            pStmt.setString(3, statusThrowable);
            pStmt.setTimestamp(4, Timestamp.from(statusTime));
            pStmt.setString(5, instanceId);
            return pStmt.executeUpdate() == 1;
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean setStatus(ScheduledRunDto scheduledRunDto) {
        return setStatus(scheduledRunDto.getInstanceId(), scheduledRunDto.getStatus(), scheduledRunDto.getStatusMsg(),
                scheduledRunDto.getStatusThrowable(), scheduledRunDto.getStatusInstant());
    }

    /**
     * Get the specific {@link ScheduledRunDbo} with the specified instanceId
     * <p>
     * Used by tests
     * @param instanceId
     *         - The instanceId to retrieve the scheduled run for.
     */
    @SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    Optional<ScheduledRunDbo> getScheduleRunWithLogs(String instanceId) {
        String sql = "SELECT * FROM " + SCHEDULE_RUN_TABLE
                + " WHERE instance_id = ? ";
        try (Connection sqlConnection = _dataSource.getConnection();
             PreparedStatement pStmt = sqlConnection.prepareStatement(sql)) {
            pStmt.setString(1, instanceId);

            // First get the logEntries for this instance:
            List<LogEntryDbo> logEntries = getLogEntriesDbo(instanceId);
            try (ResultSet result = pStmt.executeQuery()) {
                // ?: Did we find any row?
                if (result.first()) {
                    // -> Yes we found the first row
                    return Optional.of(new ScheduledRunDbo(
                            result.getString("schedule_name"),
                            result.getString("instance_id"),
                            result.getString("status"),
                            result.getString("status_msg"),
                            result.getString("status_throwable"),
                            result.getTimestamp("run_start"),
                            result.getTimestamp("status_time"),
                            logEntries));
                }

                // E-> No, we did not find anything
                return Optional.empty();
            }
        }
        catch (SQLException throwables) {
            throw new RuntimeException(throwables);
        }
    }

    /**
     * Get the specific {@link ScheduledRunDbo} with the specified instanceId
     * Note this does not load the logs of the schedule run. Use {@link #getLogEntries(String)} to fetch these.
     * @param instanceId
     *         - The instanceId to retrieve the scheduled run for.
     * @return
     */
    @SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    public Optional<ScheduledRunDto> getScheduleRun(String instanceId) {
        String sql = "SELECT * FROM " + SCHEDULE_RUN_TABLE
                + " WHERE instance_id = ? ";
        try (Connection sqlConnection = _dataSource.getConnection();
             PreparedStatement pStmt = sqlConnection.prepareStatement(sql)) {
            pStmt.setString(1, instanceId);

            try (ResultSet result = pStmt.executeQuery()) {
                // ?: Did we find any row?
                if (result.first()) {
                    // -> Yes we found the first row
                    ScheduledRunDbo scheduledRun = new ScheduledRunDbo(
                            result.getString("schedule_name"),
                            result.getString("instance_id"),
                            result.getString("status"),
                            result.getString("status_msg"),
                            result.getString("status_throwable"),
                            result.getTimestamp("run_start"),
                            result.getTimestamp("status_time"),
                            Collections.emptyList());
                    return Optional.of(ScheduledRunDto.fromDbo(scheduledRun));
                }

                // E-> No, we did not find anything
                return Optional.empty();
            }
        }
        catch (SQLException throwables) {
            throw new RuntimeException(throwables);
        }
    }


    /**
     * Get the last inserted ScheduleRun for the given scheduleName.
     * <p>
     * Note this does not load the logs of the schedule run. Use {@link #getLogEntries(String)} to fetch these.
     *
     * @param scheduleName
     *         - Name of the schedule to retrieve the last run from
     */
    @SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    public Optional<ScheduledRunDto> getLastRunForSchedule(String scheduleName) {
        String sql = "SELECT TOP(1) * FROM " + SCHEDULE_RUN_TABLE
                + " WHERE schedule_name = ? "
                + " ORDER BY run_start DESC";

        try (Connection sqlConnection = _dataSource.getConnection();
             PreparedStatement pStmt = sqlConnection.prepareStatement(sql)) {
            pStmt.setString(1, scheduleName);

            try (ResultSet result = pStmt.executeQuery()) {
                // ?: Did we find any row (we should only find one row)?
                if (result.first()) {
                    // -> Yes we found the first row
                    ScheduledRunDbo scheduleRunDbo = new ScheduledRunDbo(
                            result.getString("schedule_name"),
                            result.getString("instance_id"),
                            result.getString("status"),
                            result.getString("status_msg"),
                            result.getString("status_throwable"),
                            result.getTimestamp("run_start"),
                            result.getTimestamp("status_time"),
                            Collections.emptyList());
                    return Optional.of(ScheduledRunDto.fromDbo(scheduleRunDbo));
                }

                // E-> No, we did not find anything
                return Optional.empty();
            }
        }
        catch (SQLException throwables) {
            throw new RuntimeException(throwables);
        }
    }

    /**
     * Retrieves all the last {@link ScheduledRunDbo} for all schedule names.
     * The results are sorted by {@link ScheduledRunDbo#getRunStart()} descending.
     * <p>
     * Note this does not load the logs of the schedule run. Use {@link #getLogEntries(String)} to fetch these.
     */
    @SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    public List<ScheduledRunDto> getLastScheduleRuns() {
        String sql = " SELECT * "
                + " FROM (SELECT SCHEDULE_NAME, max(run_start) as run_start "
                + "     from " + SCHEDULE_RUN_TABLE
                + "     group by SCHEDULE_NAME) AS lr "
                + " INNER JOIN  " + SCHEDULE_RUN_TABLE + " as sr "
                + "     ON sr.SCHEDULE_NAME = lr.SCHEDULE_NAME AND sr.run_start = lr.run_start"
                + " ORDER BY run_start DESC ";

        try (Connection sqlConnection = _dataSource.getConnection();
             PreparedStatement pStmt = sqlConnection.prepareStatement(sql);
             ResultSet result = pStmt.executeQuery()) {

            List<ScheduledRunDbo> scheduledRuns = new ArrayList<>();

            while (result.next()) {
                ScheduledRunDbo scheduledRun = new ScheduledRunDbo(
                        result.getString("schedule_name"),
                        result.getString("instance_id"),
                        result.getString("status"),
                        result.getString("status_msg"),
                        result.getString("status_throwable"),
                        result.getTimestamp("run_start"),
                        result.getTimestamp("status_time"),
                        Collections.emptyList());
                scheduledRuns.add(scheduledRun);
            }

            return scheduledRuns.stream()
                        .map(scheduledRunDbo -> ScheduledRunDto.fromDbo(scheduledRunDbo))
                        .collect(toList());
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Get all {@link ScheduledRunDbo} between a given timespan.
     * Note this does not load the logs of the schedule run. Use {@link #getLogEntries(String)} to fetch these.
     * @param from
     *         - from time and including
     * @param to
     *         - to and including
     */
    @SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    public List<ScheduledRunDto> getScheduleRunsBetween(String scheduleName, LocalDateTime from, LocalDateTime to) {
        String sql = "SELECT * FROM " + SCHEDULE_RUN_TABLE
                + " WHERE run_start >= ? "
                + " AND run_start <= ? "
                + " AND schedule_name = ? "
                + " ORDER BY run_start ASC";

        try (Connection sqlConnection = _dataSource.getConnection();
             PreparedStatement pStmt = sqlConnection.prepareStatement(sql)) {
            pStmt.setTimestamp(1, Timestamp.valueOf(from));
            pStmt.setTimestamp(2, Timestamp.valueOf(to));
            pStmt.setString(3, scheduleName);

            try (ResultSet result = pStmt.executeQuery()) {
                List<ScheduledRunDbo> scheduledRuns = new ArrayList<>();

                while (result.next()) {
                    ScheduledRunDbo scheduledRun = new ScheduledRunDbo(
                            result.getString("schedule_name"),
                            result.getString("instance_id"),
                            result.getString("status"),
                            result.getString("status_msg"),
                            result.getString("status_throwable"),
                            result.getTimestamp("run_start"),
                            result.getTimestamp("status_time"),
                            Collections.emptyList());
                    scheduledRuns.add(scheduledRun);
                }

                return scheduledRuns.stream()
                        .map(scheduledRunDbo -> ScheduledRunDto.fromDbo(scheduledRunDbo))
                        .collect(toList());
            }
        }
        catch (SQLException throwables) {
            throw new RuntimeException(throwables);
        }
    }


    /**
     * Add a {@link LogEntry} to a specified ScheduleRun by using the scheduleRun's instanceId.
     *
     * @param instanceId
     *         - InstanceId for the schedule run to add the logs to
     * @param logEntry
     *         - A {@link LogEntry} to insert.
     * @return int - amount of inserts.
     */
    @SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    public int addLogEntry(String instanceId, LogEntry logEntry) {
        String sql = "INSERT INTO " + SCHEDULE_LOG_ENTRY_TABLE
                + " (instance_id, log_msg, log_throwable, log_time) "
                + " VALUES (?, ?, ?, ?)";

        log.debug("Adding logEntry for instanceId [" + instanceId + "], "
                + "logMsg [" + logEntry.getMessage() + "], throwable set [" + logEntry.getThrowable().isPresent()
                + "]");

        try (Connection sqlConnection = _dataSource.getConnection();
             PreparedStatement pStmt = sqlConnection.prepareStatement(sql)) {

            pStmt.setString(1, instanceId);
            pStmt.setString(2, logEntry.getMessage());
            pStmt.setString(3, logEntry.getThrowable().orElse(null));
            pStmt.setTimestamp(4, Timestamp.valueOf(logEntry.getLogTime()));
            return pStmt.executeUpdate();
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Get all logEntries for a given schedule run instance.
     *
     * @param instanceId
     *         - InstanceId for the schedule run to add the logs to
     * @return List<LogEntryDbo> - The logEntries (if any) for that schedule run instance
     */
    public List<LogEntry> getLogEntries(String instanceId) {
        return getLogEntriesDbo(instanceId).stream()
                .map(logEntryDbo -> LogEntryImpl.fromDbo(logEntryDbo))
                .collect(toList());
    }

    @SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    private List<LogEntryDbo> getLogEntriesDbo(String instanceId) {
        String sql = "SELECT * FROM " + SCHEDULE_LOG_ENTRY_TABLE
                + " WHERE instance_id = ? "
                + " ORDER BY log_time ASC ";
        try (Connection sqlConnection = _dataSource.getConnection();
             PreparedStatement pStmt = sqlConnection.prepareStatement(sql)) {
            pStmt.setString(1, instanceId);

            List<LogEntryDbo> logEntries = new ArrayList<>();
            try (ResultSet result = pStmt.executeQuery()) {
                while (result.next()) {
                    // -> Yes we found the first row
                    LogEntryDbo logEntry = new LogEntryDbo(
                            result.getString("instance_id"),
                            result.getString("log_msg"),
                            result.getString("log_throwable"),
                            result.getTimestamp("log_time"));
                    logEntries.add(logEntry);
                }

                return logEntries;
            }
        }
        catch (SQLException throwables) {
            throw new RuntimeException(throwables);
        }
    }

    // ===== Table validation =================================================================

    /**
     * Responsible of validating the {@link #SCHEDULE_TASK_TABLE} structure and the tableVersion flag.
     */
    private void validateScheduledTaskTableStructure() {
        TableInspector inspector = new TableInspector(_dataSource, SCHEDULE_TASK_TABLE);
        // Verify that we have a valid table
        if (inspector.amountOfColumns() == 0) {
            // Table was not found
            throw new TableValidationException("Table '" + SCHEDULE_TASK_TABLE + "' where not found, "
                    + "create the tables by manually importing '" + inspector.getMigrationFileLocation() + "'");
        }

        // ----- We got the same version as we expected, but do a sanity check regardless.
        // :: Verify we have all the table columns and their sizes
        inspector.validateColumn("schedule_name", 255, false,
                JDBCType.VARCHAR, JDBCType.NVARCHAR, JDBCType.LONGVARCHAR, JDBCType.LONGNVARCHAR);

        inspector.validateColumn("is_active", false,
                JDBCType.BOOLEAN, JDBCType.BIT, JDBCType.TINYINT, JDBCType.SMALLINT, JDBCType.INTEGER, JDBCType.NUMERIC);

        inspector.validateColumn("run_once", false,
                JDBCType.BOOLEAN, JDBCType.BIT, JDBCType.TINYINT, JDBCType.SMALLINT, JDBCType.INTEGER, JDBCType.NUMERIC);

        inspector.validateColumn("cron_expression", 255, true,
                JDBCType.VARCHAR, JDBCType.NVARCHAR, JDBCType.LONGVARCHAR, JDBCType.LONGNVARCHAR);

        inspector.validateColumn("next_run", false,
                JDBCType.TIMESTAMP, JDBCType.TIME, JDBCType.TIME_WITH_TIMEZONE, JDBCType.TIMESTAMP_WITH_TIMEZONE);

        inspector.validateColumn("last_updated", false,
                JDBCType.TIMESTAMP, JDBCType.TIME, JDBCType.TIME_WITH_TIMEZONE, JDBCType.TIMESTAMP_WITH_TIMEZONE);
    }

    /**
     * Responsible of validating the {@link #SCHEDULE_RUN_TABLE} structure
     */
    private void validateScheduleRunTableStructure() {
        TableInspector inspector = new TableInspector(_dataSource, SCHEDULE_RUN_TABLE);
        // Verify that we have a valid table
        if (inspector.amountOfColumns() == 0) {
            // Table was not found
            throw new TableValidationException("Table '" + SCHEDULE_RUN_TABLE + "' where not found, "
                    + "create the tables by manually importing '" + inspector.getMigrationFileLocation() + "'");
        }

        // :: Verify we have all the table columns and their sizes
        inspector.validateColumn("schedule_name", 255, false,
                JDBCType.VARCHAR, JDBCType.NVARCHAR, JDBCType.LONGVARCHAR, JDBCType.LONGNVARCHAR);

        inspector.validateColumn("instance_id", 255, false,
                JDBCType.VARCHAR, JDBCType.NVARCHAR, JDBCType.LONGVARCHAR, JDBCType.LONGNVARCHAR);

        inspector.validateColumn("run_start", false,
                JDBCType.TIMESTAMP, JDBCType.TIME, JDBCType.TIME_WITH_TIMEZONE, JDBCType.TIMESTAMP_WITH_TIMEZONE);

        inspector.validateColumn("status", 250, true,
                JDBCType.VARCHAR, JDBCType.NVARCHAR, JDBCType.LONGVARCHAR, JDBCType.LONGNVARCHAR);

        inspector.validateColumn("status_msg", 255, true,
                JDBCType.VARCHAR, JDBCType.NVARCHAR, JDBCType.LONGVARCHAR, JDBCType.LONGNVARCHAR);

        // Note, yes we are setting max in the migration script but due to h2 reports MAX is 2147483647
        // while JDBC driver 2 reports 1073741823 and jdbc driver 3.0 reports 2147483647
        // See https://docs.microsoft.com/en-us/sql/connect/jdbc/reference/getcolumns-method-sqlserverdatabasemetadata?view=sql-server-ver15
        // so we set the minimum to 3000
        inspector.validateColumn("status_throwable", 3000, true,
                JDBCType.VARCHAR, JDBCType.NVARCHAR, JDBCType.LONGVARCHAR, JDBCType.LONGNVARCHAR);

        inspector.validateColumn("status_time", false,
                JDBCType.TIMESTAMP, JDBCType.TIME, JDBCType.TIME_WITH_TIMEZONE, JDBCType.TIMESTAMP_WITH_TIMEZONE);
    }

    /**
     * Responsible of validating the {@link #SCHEDULE_LOG_ENTRY_TABLE} structure
     */
    private void validateScheduleLogEntryTableStructure() {
        TableInspector inspector = new TableInspector(_dataSource, SCHEDULE_LOG_ENTRY_TABLE);

        // Verify that we have a valid table
        if (inspector.amountOfColumns() == 0) {
            // Table was not found
            throw new TableValidationException("Table '" + SCHEDULE_LOG_ENTRY_TABLE + "' where not found, "
                    + "create the tables by manually importing '" + inspector.getMigrationFileLocation() + "'");
        }

        // :: Verify we have all the table columns and their sizes
        inspector.validateColumn("instance_id", 255, false,
                JDBCType.VARCHAR, JDBCType.NVARCHAR, JDBCType.LONGVARCHAR, JDBCType.LONGNVARCHAR);

        inspector.validateColumn("log_msg", 255, false,
                JDBCType.VARCHAR, JDBCType.NVARCHAR, JDBCType.LONGVARCHAR, JDBCType.LONGNVARCHAR);

        // Note, yes we are setting max in the migration script but due to h2 reports MAX is 2147483647
        // while JDBC driver 2 reports 1073741823 and jdbc driver 3.0 reports 2147483647
        // See https://docs.microsoft.com/en-us/sql/connect/jdbc/reference/getcolumns-method-sqlserverdatabasemetadata?view=sql-server-ver15
        // so we set the minimum to 3000
        inspector.validateColumn("log_throwable", 3000, true,
                JDBCType.VARCHAR, JDBCType.NVARCHAR, JDBCType.LONGVARCHAR, JDBCType.LONGNVARCHAR);

        inspector.validateColumn("log_time", false,
                JDBCType.TIMESTAMP, JDBCType.TIME, JDBCType.TIME_WITH_TIMEZONE, JDBCType.TIMESTAMP_WITH_TIMEZONE);
    }
    // ===== DBO ==============================================================================

    /**
     * Retrieve the settings for a given schedule. Like when to run, if it should run once. A cronExpression..
     */
    static class ScheduleDbo {
        private final String scheduleName;
        private final boolean active;
        private final boolean runOnce;
        private final String cronExpression;
        private final Timestamp nextRun;
        private final Timestamp lastUpdated;

        ScheduleDbo(String scheduleName, boolean active, boolean runOnce, String cronExpression, Timestamp nextRun,
                Timestamp lastUpdated) {
            this.scheduleName = scheduleName;
            this.active = active;
            this.runOnce = runOnce;
            this.cronExpression = cronExpression;
            this.nextRun = nextRun;
            this.lastUpdated = lastUpdated;
        }

        public String getScheduleName() {
            return scheduleName;
        }

        public boolean isActive() {
            return active;
        }

        public boolean isRunOnce() {
            return runOnce;
        }

        public CronExpression getCronExpression() {
            return cronExpression == null
                    ? null
                    : CronExpression.parse(cronExpression);

        }

        public Instant getNextRun() {
            return nextRun.toInstant();
        }

        public LocalDateTime getNextRunTime() {
            return nextRun.toLocalDateTime();
        }

        public Instant getLastUpdated() {
            return lastUpdated.toInstant();
        }

        public LocalDateTime getLastUpdatedTime() {
            return lastUpdated.toLocalDateTime();
        }
    }

    /**
     * DBO that holds the status for a historic Schedule run. It may also hold a current running schedule where the
     * logs are still being appended to.
     */
    static class ScheduledRunDbo {
        private final String scheduleName;
        private final String instanceId;
        private final String status;
        private final String statusMsg;
        private final String statusThrowable;
        private final Timestamp runStart;
        private final Timestamp statusTime;
        private final List<LogEntryDbo> logEntries;

        ScheduledRunDbo(String scheduleName, String instanceId, String status, String statusMsg,
                String statusThrowable, Timestamp runStart, Timestamp statusTime, List<LogEntryDbo> logEntries) {
            this.scheduleName = scheduleName;
            this.instanceId = instanceId;
            this.status = status;
            this.statusMsg = statusMsg;
            this.statusThrowable = statusThrowable;
            this.runStart = runStart;
            this.statusTime = statusTime;
            this.logEntries = logEntries;
        }

        public String getScheduleName() {
            return scheduleName;
        }

        public String getInstanceId() {
            return instanceId;
        }

        public State getStatus() {
            return State.valueOf(status);
        }

        public String getStatusMsg() {
            return statusMsg;
        }

        public String getStatusThrowable() {
            return statusThrowable;
        }

        public Instant getRunStart() {
            return runStart.toInstant();
        }

        public Instant getStatusTime() {
            return statusTime.toInstant();
        }

        public List<LogEntryDbo> getLogEntries() {
            return logEntries;
        }
    }

    /**
     * A log line for a given Schedule run.
     */
    static class LogEntryDbo {
        private final String instanceId;
        private final String logMessage;
        private final String logThrowable;
        private final Timestamp logTime;

        LogEntryDbo(String instanceId, String logMessage, String logThrowable, Timestamp logTime) {
            this.instanceId = instanceId;
            this.logMessage = logMessage;
            this.logThrowable = logThrowable;
            this.logTime = logTime;
        }

        public String getInstanceId() {
            return instanceId;
        }

        public String getLogMessage() {
            return logMessage;
        }

        public String getLogThrowable() {
            return logThrowable;
        }

        public LocalDateTime getLogTime() {
            return logTime.toLocalDateTime();
        }
    }
}
