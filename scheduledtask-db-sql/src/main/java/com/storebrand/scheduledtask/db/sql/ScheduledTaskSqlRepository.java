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

package com.storebrand.scheduledtask.db.sql;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.storebrand.scheduledtask.ScheduledTaskRegistry.LogEntry;
import com.storebrand.scheduledtask.ScheduledTask.RetentionPolicy;
import com.storebrand.scheduledtask.ScheduledTaskRegistry;
import com.storebrand.scheduledtask.ScheduledTaskRegistry.Schedule;
import com.storebrand.scheduledtask.ScheduledTaskRegistry.State;
import com.storebrand.scheduledtask.ScheduledTaskRegistryImpl;
import com.storebrand.scheduledtask.db.sql.TableInspector.TableValidationException;
import com.storebrand.scheduledtask.db.ScheduledTaskRepository;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Handles storing and updating of the {@link ScheduledTaskRegistryImpl} schedules and run logs.
 *
 * @author Dag Bertelsen - dag.lennart.bertelsen@storebrand.no - dabe@dagbertelsen.com - 2021.02
 * @author Kristian Hiim
 */
public class ScheduledTaskSqlRepository implements ScheduledTaskRepository {
    private static final Logger log = LoggerFactory.getLogger(ScheduledTaskSqlRepository.class);
    public static final String SCHEDULE_TASK_TABLE = "stb_schedule";
    public static final String SCHEDULE_RUN_TABLE = "stb_schedule_run";
    public static final String SCHEDULE_LOG_ENTRY_TABLE = "stb_schedule_log_entry";
    private final DataSource _dataSource;
    private final Clock _clock;

    @SuppressFBWarnings(value = "EI_EXPOSE_REP2", justification = "This is standard dependency injection.")
    public ScheduledTaskSqlRepository(DataSource dataSource, Clock clock) {
        _dataSource = dataSource;
        _clock = clock;

        validateScheduledTaskTableStructure();
        validateScheduleRunTableStructure();
        validateScheduleLogEntryTableStructure();
    }

    @Override
    @SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    public int createSchedule(String scheduleName, String cronExpression, Instant nextRun) {
        String sql = "INSERT INTO " + SCHEDULE_TASK_TABLE
                + " (schedule_name, is_active, run_once, cron_expression, next_run, last_updated) "
                + " SELECT ?, ?, ?, ?, ?, ? "
                + " WHERE NOT EXISTS (SELECT schedule_name FROM " + SCHEDULE_TASK_TABLE
                + " WHERE schedule_name = ?)";

        log.debug("Trying to create schedule [" + scheduleName + "]");

        try (Connection sqlConnection = _dataSource.getConnection();
             PreparedStatement pStmt = sqlConnection.prepareStatement(sql)) {
            pStmt.setString(1, scheduleName);
            // All schedules when created is by default active
            pStmt.setBoolean(2, true);
            // All new schedules should not have run once set
            pStmt.setBoolean(3, false);
            pStmt.setString(4, cronExpression);
            pStmt.setTimestamp(5, Timestamp.from(nextRun));
            pStmt.setTimestamp(6, Timestamp.from(_clock.instant()));
            pStmt.setString(7, scheduleName);
            return pStmt.executeUpdate();
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    @SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    public int updateNextRun(String scheduleName, String overrideCronExpression, Instant nextRun) {
        // :? Check if we should insert the schedule first:
        if (createSchedule(scheduleName, overrideCronExpression, nextRun) == 1) {
            // -> Yes, we managed to insert the schedule so no need of doing an update.
            return 1;
        }

        // E-> Schedule already exists so we need to update it.
        String sql = "UPDATE " + SCHEDULE_TASK_TABLE
                + " SET cron_expression = ?, next_run = ?, last_updated = ? "
                + " WHERE schedule_name = ?";

        log.info("Updating next run to [" + scheduleName + "] cronExpression [" + overrideCronExpression + "] "
                + "and nextRun [" + nextRun + "]");

        try (Connection sqlConnection = _dataSource.getConnection();
             PreparedStatement pStmt = sqlConnection.prepareStatement(sql)) {
            pStmt.setString(1, overrideCronExpression);
            pStmt.setTimestamp(2, Timestamp.from(nextRun));
            pStmt.setTimestamp(3, Timestamp.from(_clock.instant()));
            pStmt.setString(4, scheduleName);
            return pStmt.executeUpdate();
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    @SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    public int setActive(String scheduleName, boolean active) {
        String sql = "UPDATE " + SCHEDULE_TASK_TABLE
                + " SET is_active = ? "
                + " WHERE schedule_name = ?";

        log.info("Schedule [" + scheduleName + "] is now set to be isActive [" + active + "] ");

        try (Connection sqlConnection = _dataSource.getConnection();
             PreparedStatement pStmt = sqlConnection.prepareStatement(sql)) {
            pStmt.setBoolean(1, active);
            pStmt.setString(2, scheduleName);
            return pStmt.executeUpdate();
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    @SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    public int setRunOnce(String scheduleName, boolean runOnce) {
        String sql = "UPDATE " + SCHEDULE_TASK_TABLE
                + " SET run_once = ? "
                + " WHERE schedule_name = ?";

        log.info("Schedule [" + scheduleName + "] is now set to be isRunOnce [" + runOnce + "] ");

        try (Connection sqlConnection = _dataSource.getConnection();
             PreparedStatement pStmt = sqlConnection.prepareStatement(sql)) {
            pStmt.setBoolean(1, runOnce);
            pStmt.setString(2, scheduleName);
            return pStmt.executeUpdate();
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    @SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    public Map<String, Schedule> getSchedules() {
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
                    .map(ScheduledTaskSqlRepository::fromDbo)
                    .collect(toMap(Schedule::getName, s -> s));
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    @SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    public Optional<Schedule> getSchedule(String scheduleName) {
        String sql = "SELECT * FROM " + SCHEDULE_TASK_TABLE
                + " WHERE schedule_name = ? ";

        try (Connection sqlConnection = _dataSource.getConnection();
             PreparedStatement pStmt = sqlConnection.prepareStatement(sql)) {
            pStmt.setString(1, scheduleName);
            try (ResultSet result = pStmt.executeQuery()) {
                // ?: Did we find any row?
                if (result.next()) {
                    // -> Yes we found the first row
                    ScheduleDbo scheduleDbo = new ScheduleDbo(
                            result.getString("schedule_name"),
                            result.getBoolean("is_active"),
                            result.getBoolean("run_once"),
                            result.getString("cron_expression"),
                            result.getTimestamp("next_run"),
                            result.getTimestamp("last_updated"));
                    return Optional.of(fromDbo(scheduleDbo));
                }

                // E-> No, we did not find anything
                return Optional.empty();
            }
        }
        catch (SQLException throwables) {
            throw new RuntimeException(throwables);
        }
    }

    @Override
    @SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    public long addScheduleRun(String scheduleName, String hostname, Instant runStart, String statusMsg) {
        String sql = "INSERT INTO " + SCHEDULE_RUN_TABLE
                + " (schedule_name, hostname, status, status_msg, run_start, status_time) "
                + " VALUES (?, ?, ?, ?, ?, ?)";

        log.debug("Adding scheduleRun for scheuleName [" + scheduleName + "]");

        try (Connection sqlConnection = _dataSource.getConnection();
             PreparedStatement pStmt = sqlConnection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            pStmt.setString(1, scheduleName);
            pStmt.setString(2, hostname);
            pStmt.setString(3, ScheduledTaskRegistry.State.STARTED.toString());
            pStmt.setString(4, statusMsg);
            pStmt.setTimestamp(5, Timestamp.from(runStart));
            pStmt.setTimestamp(6, Timestamp.from(_clock.instant()));
            pStmt.execute();
            try (ResultSet rs = pStmt.getGeneratedKeys()) {
                if (rs.next()) {
                    return rs.getLong(1);
                }
            }
            throw new IllegalStateException("Unable to determine runId for new scheduleRun for scheduleName ["
                    + scheduleName + "]");
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    @SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    public boolean setStatus(long runId, State state, String statusMsg, String statusStackTrace,
            Instant statusTime) {
        String sql = "UPDATE " + SCHEDULE_RUN_TABLE
                + " SET status = ?, status_msg = ?, status_stacktrace = ?, status_time = ? "
                + " WHERE run_id = ?";

        if (state.equals(ScheduledTaskRegistry.State.STARTED)) {
            throw new IllegalArgumentException("The state STARTED can only be set during the addScheduleRun");
        }

        try (Connection sqlConnection = _dataSource.getConnection();
             PreparedStatement pStmt = sqlConnection.prepareStatement(sql)) {

            // We should only allow to acquire the lock if the last_updated_time is older than 10 minutes.
            // Then it means it is up for grabs.
            pStmt.setString(1, state.toString());
            pStmt.setString(2, statusMsg);
            pStmt.setString(3, statusStackTrace);
            pStmt.setTimestamp(4, Timestamp.from(statusTime));
            pStmt.setLong(5, runId);
            return pStmt.executeUpdate() == 1;
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean setStatus(ScheduledRunDto scheduledRunDto) {
        return setStatus(scheduledRunDto.getRunId(), scheduledRunDto.getStatus(), scheduledRunDto.getStatusMsg(),
                scheduledRunDto.getStatusStackTrace(), scheduledRunDto.getStatusInstant());
    }

    /**
     * Get the specific {@link ScheduledRunDbo} with the specified instanceId
     * <p>
     * Used by tests
     * @param runId
     *         - The runId to retrieve the scheduled run for.
     */
    @SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    Optional<ScheduledRunDbo> getScheduleRunWithLogs(long runId) {
        String sql = "SELECT * FROM " + SCHEDULE_RUN_TABLE
                + " WHERE run_id = ? ";
        try (Connection sqlConnection = _dataSource.getConnection();
             PreparedStatement pStmt = sqlConnection.prepareStatement(sql)) {
            pStmt.setLong(1, runId);

            // First get the logEntries for this instance:
            List<LogEntry> logEntries = getLogEntries(runId);
            try (ResultSet result = pStmt.executeQuery()) {
                // ?: Did we find any row?
                if (result.next()) {
                    // -> Yes we found the first row
                    return Optional.of(fromResultSet(result, logEntries));
                }

                // E-> No, we did not find anything
                return Optional.empty();
            }
        }
        catch (SQLException throwables) {
            throw new RuntimeException(throwables);
        }
    }

    @Override
    @SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    public Optional<ScheduledRunDto> getScheduleRun(long runId) {
        String sql = "SELECT * FROM " + SCHEDULE_RUN_TABLE
                + " WHERE run_id = ? ";
        try (Connection sqlConnection = _dataSource.getConnection();
             PreparedStatement pStmt = sqlConnection.prepareStatement(sql)) {
            pStmt.setLong(1, runId);

            try (ResultSet result = pStmt.executeQuery()) {
                // ?: Did we find any row?
                if (result.next()) {
                    // -> Yes we found the first row
                    return Optional.of(fromDbo(fromResultSet(result, List.of())));
                }

                // E-> No, we did not find anything
                return Optional.empty();
            }
        }
        catch (SQLException throwables) {
            throw new RuntimeException(throwables);
        }
    }


    @Override
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
                if (result.next()) {
                    // -> Yes we found the first row
                    return Optional.of(fromDbo(fromResultSet(result, List.of())));
                }

                // E-> No, we did not find anything
                return Optional.empty();
            }
        }
        catch (SQLException throwables) {
            throw new RuntimeException(throwables);
        }
    }

    @Override
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
                ScheduledRunDbo scheduledRun = fromResultSet(result, List.of());
                scheduledRuns.add(scheduledRun);
            }

            return scheduledRuns.stream()
                        .map(ScheduledTaskSqlRepository::fromDbo)
                        .collect(toList());
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    @SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    public List<ScheduledRunDto> getScheduleRunsBetween(String scheduleName, LocalDateTime from, LocalDateTime to) {
        String sql = "SELECT * FROM " + SCHEDULE_RUN_TABLE
                + " WHERE run_start >= ? "
                + " AND run_start <= ? "
                + " AND schedule_name = ? "
                + " ORDER BY run_start DESC";

        try (Connection sqlConnection = _dataSource.getConnection();
             PreparedStatement pStmt = sqlConnection.prepareStatement(sql)) {
            pStmt.setTimestamp(1, Timestamp.valueOf(from));
            pStmt.setTimestamp(2, Timestamp.valueOf(to));
            pStmt.setString(3, scheduleName);

            try (ResultSet result = pStmt.executeQuery()) {
                List<ScheduledRunDbo> scheduledRuns = new ArrayList<>();

                while (result.next()) {
                    ScheduledRunDbo scheduledRun = fromResultSet(result, List.of());
                    scheduledRuns.add(scheduledRun);
                }

                return scheduledRuns.stream()
                        .map(ScheduledTaskSqlRepository::fromDbo)
                        .collect(toList());
            }
        }
        catch (SQLException throwables) {
            throw new RuntimeException(throwables);
        }
    }

    @Override
    @SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    public void addLogEntry(long runId, LocalDateTime logTime, String message, String stackTrace) {
        String sql = "INSERT INTO " + SCHEDULE_LOG_ENTRY_TABLE
                + " (run_id, log_msg, log_stacktrace, log_time) "
                + " VALUES (?, ?, ?, ?)";

        log.debug("Adding logEntry for runId [" + runId + "], "
                + "logMsg [" + message + "], stack trace set [" + (stackTrace != null) + "]");

        try (Connection sqlConnection = _dataSource.getConnection();
             PreparedStatement pStmt = sqlConnection.prepareStatement(sql)) {

            pStmt.setLong(1, runId);
            pStmt.setString(2, message);
            pStmt.setString(3, stackTrace);
            pStmt.setTimestamp(4, Timestamp.valueOf(logTime));
            pStmt.executeUpdate();
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    @SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    public List<LogEntry> getLogEntries(long runId) {
        String sql = "SELECT * FROM " + SCHEDULE_LOG_ENTRY_TABLE
                + " WHERE run_id = ? "
                + " ORDER BY log_time ASC ";
        try (Connection sqlConnection = _dataSource.getConnection();
             PreparedStatement pStmt = sqlConnection.prepareStatement(sql)) {
            pStmt.setLong(1, runId);

            List<LogEntry> logEntries = new ArrayList<>();
            try (ResultSet result = pStmt.executeQuery()) {
                while (result.next()) {
                    // -> Yes we found the first row
                    LogEntryDbo logEntry = new LogEntryDbo(
                            result.getLong("log_id"),
                            result.getLong("run_id"),
                            result.getString("log_msg"),
                            result.getString("log_stacktrace"),
                            result.getTimestamp("log_time"));
                    logEntries.add(logEntry);
                }

                return Collections.unmodifiableList(logEntries);
            }
        }
        catch (SQLException throwables) {
            throw new RuntimeException(throwables);
        }
    }

    private ScheduledRunDbo fromResultSet(ResultSet result, List<LogEntry> logEntries) throws SQLException {
        return new ScheduledRunDbo(
                result.getLong("run_id"),
                result.getString("schedule_name"),
                result.getString("hostname"),
                result.getString("status"),
                result.getString("status_msg"),
                result.getString("status_stacktrace"),
                result.getTimestamp("run_start"),
                result.getTimestamp("status_time"),
                logEntries);
    }

    // ===== Retention policy =================================================================

    @Override
    public void executeRetentionPolicy(String scheduleName, RetentionPolicy retentionPolicy) {
        if (!retentionPolicy.isRetentionPolicyEnabled()) {
            return;
        }

        int deletedRecords = 0;
        try (Connection sqlConnection = _dataSource.getConnection()) {

            // ?: Is delete runs after days defined?
            if (retentionPolicy.getDeleteRunsAfterDays() > 0) {
                // -> Yes, then we delete all records older than max days.

                LocalDateTime deleteOlder = LocalDateTime.now(_clock)
                        .minusDays(retentionPolicy.getDeleteRunsAfterDays());

                deletedRecords += executeDelete(sqlConnection, scheduleName, deleteOlder, null);
            }

            // ?: Is delete successful runs after days defined?
            if (retentionPolicy.getDeleteSuccessfulRunsAfterDays() > 0) {
                // -> Yes, then we delete all records older than max days.

                LocalDateTime deleteOlder = LocalDateTime.now(_clock)
                        .minusDays(retentionPolicy.getDeleteSuccessfulRunsAfterDays());

                deletedRecords += executeDelete(sqlConnection, scheduleName, deleteOlder, State.DONE);
            }

            // ?: Is delete failed runs after days defined?
            if (retentionPolicy.getDeleteFailedRunsAfterDays() > 0) {
                // -> Yes, then we delete all records older than max days.

                LocalDateTime deleteOlder = LocalDateTime.now(_clock)
                        .minusDays(retentionPolicy.getDeleteFailedRunsAfterDays());

                deletedRecords += executeDelete(sqlConnection, scheduleName, deleteOlder, State.FAILED);
            }

            // ?: Is delete noop runs after days defined?
            if (retentionPolicy.getDeleteNoopRunsAfterDays() > 0) {
                // -> Yes, then we delete all records older than max days.

                LocalDateTime deleteOlder = LocalDateTime.now(_clock)
                        .minusDays(retentionPolicy.getDeleteNoopRunsAfterDays());

                deletedRecords += executeDelete(sqlConnection, scheduleName, deleteOlder, State.NOOP);
            }

            // ?: Have we defined max runs to keep?
            if (retentionPolicy.getKeepMaxRuns() > 0) {
                // -> Yes, then should only keep this many

                Optional<LocalDateTime> deleteOlder = findDeleteOlderForKeepMax(sqlConnection, scheduleName,
                        retentionPolicy.getKeepMaxRuns(), null);

                if (deleteOlder.isPresent()) {
                    deletedRecords += executeDelete(sqlConnection, scheduleName, deleteOlder.get(), null);
                }
            }

            // ?: Have we defined max successful runs to keep?
            if (retentionPolicy.getKeepMaxSuccessfulRuns() > 0) {
                // -> Yes, then should only keep this many

                Optional<LocalDateTime> deleteOlder = findDeleteOlderForKeepMax(sqlConnection, scheduleName,
                        retentionPolicy.getKeepMaxSuccessfulRuns(), State.DONE);

                if (deleteOlder.isPresent()) {
                    deletedRecords += executeDelete(sqlConnection, scheduleName, deleteOlder.get(), State.DONE);
                }
            }

            // ?: Have we defined max failed runs to keep?
            if (retentionPolicy.getKeepMaxFailedRuns() > 0) {
                // -> Yes, then should only keep this many

                Optional<LocalDateTime> deleteOlder = findDeleteOlderForKeepMax(sqlConnection, scheduleName,
                        retentionPolicy.getKeepMaxFailedRuns(), State.FAILED);

                if (deleteOlder.isPresent()) {
                    deletedRecords += executeDelete(sqlConnection, scheduleName, deleteOlder.get(), State.FAILED);
                }
            }

            // ?: Have we defined max noop runs to keep?
            if (retentionPolicy.getKeepMaxNoopRuns() > 0) {
                // -> Yes, then should only keep this many

                Optional<LocalDateTime> deleteOlder = findDeleteOlderForKeepMax(sqlConnection, scheduleName,
                        retentionPolicy.getKeepMaxFailedRuns(), State.NOOP);

                if (deleteOlder.isPresent()) {
                    deletedRecords += executeDelete(sqlConnection, scheduleName, deleteOlder.get(), State.NOOP);
                }
            }

            if (deletedRecords > 0) {
                log.info("Scheduled task " + scheduleName + ": Deleted " + deletedRecords + " old records.");
            }
        }
        catch (Throwable ex) {
            // We should not crash out if retention policy execution fails. We just log it, and continue.
            log.error("Scheduled task " + scheduleName + ": Error executing retention policy.", ex);
            if (deletedRecords > 0) {
                log.info("Scheduled task " + scheduleName + ": Still managed to deleted "
                        + deletedRecords + " old records, even with exception thrown during execution of policy.");
            }
        }
    }

    private Optional<LocalDateTime> findDeleteOlderForKeepMax(Connection sqlConnection, String scheduleName, int keep,
            State status) throws SQLException {
        String sql = "SELECT run_start FROM " + SCHEDULE_RUN_TABLE + " WHERE schedule_name = ? ";
        // ?: Are we querying for a specific status?
        if (status != null) {
            // Yes -> Add specific status to query
            sql += " AND status = ? ";
        }
        else {
            // No -> Then we should delete DONE, NOOP and FAILED statuses. We should not delete runs that are not complete.
            sql += " AND status IN (?, ?, ?) ";
        }
        sql += " ORDER BY schedule_name, run_start DESC, status "
                + " OFFSET ? ROWS "
                + " FETCH NEXT 1 ROWS ONLY ";

        try (PreparedStatement pStmt = sqlConnection.prepareStatement(sql)) {
            pStmt.setString(1, scheduleName);
            if (status != null) {
                pStmt.setString(2, status.name());

                pStmt.setInt(3, keep);
            }
            else {
                pStmt.setString(2, State.DONE.name());
                pStmt.setString(3, State.NOOP.name());
                pStmt.setString(4, State.FAILED.name());

                pStmt.setInt(5, keep);
            }

            ResultSet rs = pStmt.executeQuery();
            if (rs.next()) {
                Timestamp runStart = rs.getTimestamp("run_start");
                return Optional.of(runStart.toLocalDateTime());
            }
        }
        return Optional.empty();
    }

    private int executeDelete(Connection sqlConnection,
            String scheduleName, LocalDateTime deleteOlder, State status) throws SQLException {

        String where = " WHERE schedule_name = ?"
                + " AND run_start <= ?";
        // ?: Are we querying for a specific status?
        if (status != null) {
            // Yes -> Add specific status to query
            where += " AND status = ? ";
        }
        else {
            // No -> Then we should delete DONE, NOOP and FAILED statuses. We should not delete runs that are not complete.
            where += " AND status IN (?, ?, ?) ";
        }

        String deleteLogs = "DELETE FROM " + SCHEDULE_LOG_ENTRY_TABLE
                + " WHERE run_id IN (SELECT run_id FROM " + SCHEDULE_RUN_TABLE + where + ")";

        String deleteRuns = "DELETE FROM " + SCHEDULE_RUN_TABLE
                + where;

        try (PreparedStatement pStmt = sqlConnection.prepareStatement(deleteLogs)) {
            pStmt.setString(1, scheduleName);
            pStmt.setTimestamp(2, Timestamp.valueOf(deleteOlder));
            if (status != null) {
                pStmt.setString(3, status.name());
            }
            else {
                pStmt.setString(3, State.DONE.name());
                pStmt.setString(4, State.NOOP.name());
                pStmt.setString(5, State.FAILED.name());
            }

            pStmt.executeUpdate();
        }

        try (PreparedStatement pStmt = sqlConnection.prepareStatement(deleteRuns)) {
            pStmt.setString(1, scheduleName);
            pStmt.setTimestamp(2, Timestamp.valueOf(deleteOlder));
            if (status != null) {
                pStmt.setString(3, status.name());
            }
            else {
                pStmt.setString(3, State.DONE.name());
                pStmt.setString(4, State.FAILED.name());
                pStmt.setString(5, State.NOOP.name());
            }

            return pStmt.executeUpdate();
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
        inspector.validateColumn("run_id", false,
                JDBCType.VARCHAR, JDBCType.BIGINT, JDBCType.LONGVARCHAR, JDBCType.LONGNVARCHAR);

        inspector.validateColumn("schedule_name", 255, false,
                JDBCType.VARCHAR, JDBCType.NVARCHAR, JDBCType.LONGVARCHAR, JDBCType.LONGNVARCHAR);

        inspector.validateColumn("hostname", 255, false,
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
        inspector.validateColumn("status_stacktrace", 3000, true,
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
        inspector.validateColumn("log_id", false,
                JDBCType.VARCHAR, JDBCType.BIGINT, JDBCType.LONGVARCHAR, JDBCType.LONGNVARCHAR);

        inspector.validateColumn("run_id", false,
                JDBCType.VARCHAR, JDBCType.BIGINT, JDBCType.LONGVARCHAR, JDBCType.LONGNVARCHAR);

        inspector.validateColumn("log_msg", 255, false,
                JDBCType.VARCHAR, JDBCType.NVARCHAR, JDBCType.LONGVARCHAR, JDBCType.LONGNVARCHAR);

        // Note, yes we are setting max in the migration script but due to h2 reports MAX is 2147483647
        // while JDBC driver 2 reports 1073741823 and jdbc driver 3.0 reports 2147483647
        // See https://docs.microsoft.com/en-us/sql/connect/jdbc/reference/getcolumns-method-sqlserverdatabasemetadata?view=sql-server-ver15
        // so we set the minimum to 3000
        inspector.validateColumn("log_stacktrace", 3000, true,
                JDBCType.VARCHAR, JDBCType.NVARCHAR, JDBCType.LONGVARCHAR, JDBCType.LONGNVARCHAR);

        inspector.validateColumn("log_time", false,
                JDBCType.TIMESTAMP, JDBCType.TIME, JDBCType.TIME_WITH_TIMEZONE, JDBCType.TIMESTAMP_WITH_TIMEZONE);
    }

    private static Schedule fromDbo(ScheduleDbo dbo) {
        return new ScheduleDto(dbo.getScheduleName(), dbo.isActive(), dbo.isRunOnce(), dbo.getCronExpression(),
                dbo.getNextRun(), dbo.getLastUpdated());
    }

    static ScheduledRunDto fromDbo(ScheduledRunDbo dbo) {
        return new ScheduledRunDto(dbo.getRunId(), dbo.getScheduleName(), dbo.getHostname(), dbo.getStatus(), dbo.getStatusMsg(),
                dbo.getStatusThrowable(), dbo.getRunStart(), dbo.getStatusTime());
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

        public String getCronExpression() {
            return cronExpression;
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
        private final long runId;
        private final String scheduleName;
        private final String hostname;
        private final String status;
        private final String statusMsg;
        private final String statusThrowable;
        private final Timestamp runStart;
        private final Timestamp statusTime;
        private final List<LogEntry> logEntries;

        ScheduledRunDbo(long runId, String scheduleName, String hostname, String status, String statusMsg,
                String statusThrowable, Timestamp runStart, Timestamp statusTime, List<LogEntry> logEntries) {
            this.runId = runId;
            this.scheduleName = scheduleName;
            this.hostname = hostname;
            this.status = status;
            this.statusMsg = statusMsg;
            this.statusThrowable = statusThrowable;
            this.runStart = runStart;
            this.statusTime = statusTime;
            this.logEntries = logEntries;
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
            return ScheduledTaskRegistry.State.valueOf(status);
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

        public List<LogEntry> getLogEntries() {
            return logEntries;
        }
    }

    /**
     * A log line for a given Schedule run.
     */
    static class LogEntryDbo implements LogEntry {
        private final long _logId;
        private final long _runId;
        private final String _message;
        private final String _stackTrace;
        private final LocalDateTime _logTime;

        LogEntryDbo(long logId, long runId, String message, String stackTrace, Timestamp logTime) {
            _logId = logId;
            _runId = runId;
            _message = message;
            _stackTrace = stackTrace;
            _logTime = logTime.toLocalDateTime();
        }

        @Override
        public long getLogId() {
            return _logId;
        }

        @Override
        public long getRunId() {
            return _runId;
        }

        @Override
        public String getMessage() {
            return _message;
        }

        @Override
        public Optional<String> getStackTrace() {
            return Optional.ofNullable(_stackTrace);
        }

        @Override
        public LocalDateTime getLogTime() {
            return _logTime;
        }
    }

    // ===== DTOs ======================================================================================================

    /**
     * The schedule settings retrieved from the database.
     */
    static class ScheduleDto implements Schedule {
        private final String scheduleName;
        private final boolean active;
        private final boolean runOnce;
        private final String overriddenCronExpression;
        private final Instant nextRun;
        private final Instant lastUpdated;

        ScheduleDto(String scheduleName, boolean active, boolean runOnce, String cronExpression,
                Instant nextRun, Instant lastUpdated) {
            this.scheduleName = scheduleName;
            this.active = active;
            this.runOnce = runOnce;
            this.overriddenCronExpression = cronExpression;
            this.nextRun = nextRun;
            this.lastUpdated = lastUpdated;
        }

        @Override
        public String getName() {
            return scheduleName;
        }

        @Override
        public boolean isActive() {
            return active;
        }

        @Override
        public boolean isRunOnce() {
            return runOnce;
        }

        @Override
        public Optional<String> getOverriddenCronExpression() {
            return Optional.ofNullable(overriddenCronExpression);
        }

        @Override
        public Instant getNextRun() {
            return nextRun;
        }

        @Override
        public Instant getLastUpdated() {
            return lastUpdated;
        }
    }
}
