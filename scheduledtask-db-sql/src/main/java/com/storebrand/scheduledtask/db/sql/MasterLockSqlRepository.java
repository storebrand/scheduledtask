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

import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.storebrand.scheduledtask.ScheduledTaskRegistry.MasterLock;
import com.storebrand.scheduledtask.ScheduledTaskRegistryImpl;
import com.storebrand.scheduledtask.db.sql.TableInspector.TableValidationException;
import com.storebrand.scheduledtask.db.MasterLockRepository;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Repository used by {@link ScheduledTaskRegistryImpl} to handle master election for a node.
 * On each {@link #tryAcquireLock(String, String)} it will also try to insert the lock, if the node managed to insert it
 * then that node has the lock.
 * <p>
 * After a lock has been acquired for a node it has to do the {@link #keepLock(String, String)} within the next 5 min
 * in order to be allowed to keep it. If it does not update withing that timespan it has to wait until the
 * {@link MasterLockDto#getLockLastUpdatedTime()} is over 10 min old before any node can acquire it again. This means
 * there is a 5 min gap where no node can aquire the lock at all.
 *
 * @author Dag Bertelsen - dag.lennart.bertelsen@storebrand.no - dabe@dagbertelsen.com - 2021.03
 */
public class MasterLockSqlRepository implements MasterLockRepository {
    private static final Logger log = LoggerFactory.getLogger(MasterLockSqlRepository.class);
    public static final String MASTER_LOCK_TABLE = "stb_schedule_master_locker";
    private final DataSource _dataSource;
    private final Clock _clock;

    @SuppressFBWarnings(value = "EI_EXPOSE_REP2", justification = "This is standard dependency injection.")
    public MasterLockSqlRepository(DataSource dataSource, Clock clock) {
        _dataSource = dataSource;
        _clock = clock;

        // Make sure we have a table to use, and that it has the specification we expect it to have.
        validateTableVersion();
        validateTableStructure();
    }

    @Override
    @SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    public boolean tryCreateLock(String lockName, String nodeName) {
        String sql = "INSERT INTO " + MASTER_LOCK_TABLE
                + " (lock_name, node_name, lock_taken_time, lock_last_updated_time) "
                + " SELECT ?, ?, ?, ? "
                + " WHERE NOT EXISTS (SELECT lock_name FROM " + MASTER_LOCK_TABLE
                + " WHERE lock_name = ?)";

        log.debug("Trying to create masterLock [" + lockName + "] on node [" + nodeName + "]");

        try (Connection sqlConnection = _dataSource.getConnection();
            PreparedStatement pStmt = sqlConnection.prepareStatement(sql)) {
            pStmt.setString(1, lockName);
            pStmt.setString(2, nodeName);
            pStmt.setTimestamp(3, Timestamp.from(Instant.EPOCH));
            pStmt.setTimestamp(4, Timestamp.from(Instant.EPOCH));
            pStmt.setString(5, lockName);
            return pStmt.executeUpdate() == 1;
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    @SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    public boolean tryAcquireLock(String lockName, String nodeName) {
        // This lock already should exist so try to acquire it.
        String sql = "UPDATE " + MASTER_LOCK_TABLE
                + " SET node_name = ?, lock_taken_time = ?, lock_last_updated_time = ? "
                + " WHERE lock_name = ? "
                // (lockLastUpdated <= $now - 10 minutes)). Can only acquire lock if the lastUpdated is more than 10 min old
                + " AND lock_last_updated_time <= ?";

        try (Connection sqlConnection = _dataSource.getConnection();
            PreparedStatement pStmt = sqlConnection.prepareStatement(sql)) {
            Instant now = _clock.instant();
            // We should only allow to acquire the lock if the last_updated_time is older than 10 minutes.
            // Then it means it is up for grabs.
            Instant lockShouldBeOlderThan = now.minus(10, ChronoUnit.MINUTES);
            pStmt.setString(1, nodeName);
            pStmt.setTimestamp(2, Timestamp.from(now));
            pStmt.setTimestamp(3, Timestamp.from(now));
            pStmt.setString(4, lockName);
            pStmt.setTimestamp(5, Timestamp.from(lockShouldBeOlderThan));
            return pStmt.executeUpdate() == 1;
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    @SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    public boolean releaseLock(String lockName, String nodeName) {
        String sql = "UPDATE " + MASTER_LOCK_TABLE
                + " SET lock_taken_time = ?, lock_last_updated_time = ? "
                + " WHERE lock_name = ? "
                + " AND node_name = ?";

        try (Connection sqlConnection = _dataSource.getConnection();
            PreparedStatement pStmt = sqlConnection.prepareStatement(sql)) {
            pStmt.setTimestamp(1, Timestamp.from(Instant.EPOCH));
            pStmt.setTimestamp(2, Timestamp.from(Instant.EPOCH));
            pStmt.setString(3, lockName);
            pStmt.setString(4, nodeName);
            return pStmt.executeUpdate() == 1;
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    @SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    public boolean keepLock(String lockName, String nodeName) {
        String sql = "UPDATE " + MASTER_LOCK_TABLE
                + " SET node_name = ?,lock_last_updated_time = ? "
                + " WHERE lock_name = ? "
                + " AND node_name = ? "
                // (lockLastUpdated >= $now - 5 minutes)). Can only do keeplock within 5 min after it was last updated.
                + " AND lock_last_updated_time >= ?";

        try (Connection sqlConnection = _dataSource.getConnection();
            PreparedStatement pStmt = sqlConnection.prepareStatement(sql)) {
            Instant now = _clock.instant();
            Instant lockShouldBeNewerThan = now.minus(5, ChronoUnit.MINUTES);
            pStmt.setString(1, nodeName);
            pStmt.setTimestamp(2, Timestamp.from(now));
            pStmt.setString(3, lockName);
            pStmt.setString(4, nodeName);
            pStmt.setTimestamp(5, Timestamp.from(lockShouldBeNewerThan));
            return pStmt.executeUpdate() == 1;
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    @SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    public List<MasterLock> getLocks() throws SQLException {
        String sql = "SELECT * FROM " + MASTER_LOCK_TABLE;

        try (Connection sqlConnection = _dataSource.getConnection();
            PreparedStatement pStmt = sqlConnection.prepareStatement(sql);
             ResultSet result = pStmt.executeQuery()) {

            List<MasterLockDto> masterLocks = new ArrayList<>();
            while (result.next()) {
                MasterLockDto row = new MasterLockDto(
                        result.getString("lock_name"),
                        result.getString("node_name"),
                        result.getTimestamp("lock_taken_time"),
                        result.getTimestamp("lock_last_updated_time"));
                masterLocks.add(row);
            }
            return Collections.unmodifiableList(masterLocks);
         }
    }

    @Override
    @SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    public Optional<MasterLock> getLock(String lockName) {
        String sql = "SELECT * FROM " + MASTER_LOCK_TABLE
                + " WHERE lock_name = ? ";
        try (Connection sqlConnection = _dataSource.getConnection();
             PreparedStatement pStmt = sqlConnection.prepareStatement(sql)) {
            pStmt.setString(1, lockName);
            try (ResultSet result = pStmt.executeQuery()) {
                // ?: Did we find any row?
                if (result.next()) {
                    // -> Yes we found the first row
                    MasterLockDto dbo = new MasterLockDto(
                            result.getString("lock_name"),
                            result.getString("node_name"),
                            result.getTimestamp("lock_taken_time"),
                            result.getTimestamp("lock_last_updated_time"));
                    return Optional.of(dbo);
                }

                // E-> No, we did not find anything
                return Optional.empty();
            }
         }
        catch (SQLException throwables) {
            throw new RuntimeException(throwables);
        }
    }

    // ===== Table validation =================================================================
    private void validateTableVersion() {
        TableInspector inspector = new TableInspector(_dataSource, MASTER_LOCK_TABLE);
        // Get the tableVersion
        int version = inspector.getTableVersion();
        if (version != TableInspector.VALID_VERSION) {
            // NO-> different version than what we expected, this means these tables may not be correct.
            log.error(TableInspector.TABLE_VERSION + " has the version '" + version + "' "
                    + "while we expected '" + TableInspector.VALID_VERSION + "'. "
                    + inspector.getMigrationLocationMessage());
        }
    }

    private void validateTableStructure() {
        TableInspector inspector = new TableInspector(_dataSource, MASTER_LOCK_TABLE);
        // Verify that we have a valid table
        if (inspector.amountOfColumns() == 0) {
            // Table was not found
            throw new TableValidationException("Table '" + MASTER_LOCK_TABLE + "' where not found, "
                    + "create the tables by manually importing '" + inspector.getMigrationFileLocation() + "'");
        }

        // :: Verify we have all the table columns and their sizes
        inspector.validateColumn("lock_name", 255, false,
                JDBCType.VARCHAR, JDBCType.NVARCHAR, JDBCType.LONGVARCHAR, JDBCType.LONGNVARCHAR);

        inspector.validateColumn("node_name", 255, false,
                JDBCType.VARCHAR, JDBCType.NVARCHAR, JDBCType.LONGVARCHAR, JDBCType.LONGNVARCHAR);

        inspector.validateColumn("lock_taken_time", false,
                JDBCType.TIMESTAMP, JDBCType.TIME, JDBCType.TIME_WITH_TIMEZONE, JDBCType.TIMESTAMP_WITH_TIMEZONE);

        inspector.validateColumn("lock_last_updated_time", false,
                JDBCType.TIMESTAMP, JDBCType.TIME, JDBCType.TIME_WITH_TIMEZONE, JDBCType.TIMESTAMP_WITH_TIMEZONE);

    }

    // ===== DTO ==============================================================================

    /**
     * Simple DTO class that represents a master lock.
     */
    public static class MasterLockDto implements MasterLock {
        private final String lockName;
        private final String nodeName;
        private final Instant lockTakenTime;
        private final Instant lockLastUpdatedTime;

        MasterLockDto(String lockName, String nodeName, Timestamp lockTakenTime, Timestamp lockLastUpdatedTime) {
            this.lockName = lockName;
            this.nodeName = nodeName;
            this.lockTakenTime = lockTakenTime.toInstant();
            this.lockLastUpdatedTime = lockLastUpdatedTime.toInstant();
        }

        public String getLockName() {
            return lockName;
        }

        public String getNodeName() {
            return nodeName;
        }

        public Instant getLockTakenTime() {
            return lockTakenTime;
        }

        public Instant getLockLastUpdatedTime() {
            return lockLastUpdatedTime;
        }

        @Override
        public boolean isValid(Instant now) {
            return lockLastUpdatedTime.isAfter(now.minus(5, ChronoUnit.MINUTES));
        }
    }
}
