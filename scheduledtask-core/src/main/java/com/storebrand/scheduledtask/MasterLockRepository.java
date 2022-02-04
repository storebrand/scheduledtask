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
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.storebrand.scheduledtask.ScheduledTaskServiceImpl.MasterLockDto;
import com.storebrand.scheduledtask.TableInspector.TableValidationException;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Repository used by {@link ScheduledTaskServiceImpl} to handle master election for a node.
 * On each {@link #tryAcquireLock(String, String)} it will also try to insert the lock, if the node managed to insert it
 * then that node has the lock.
 * <p>
 * After a lock has been acquired for a node it has to do the {@link #keepLock(String, String)} within the next 5 min
 * in order to be allowed to keep it. If it does not update withing that timespan i has to wait until the
 * {@link MasterLockDbo#getLockLastUpdatedTime()} is over 10 min old before any node can acquire it again. This means
 * there is a 5 min gap where no node can aquire the lock at all.
 *
 * @author Dag Bertelsen - dag.lennart.bertelsen@storebrand.no - dabe@dagbertelsen.com - 2021.03
 */
public class MasterLockRepository {
    private static final Logger log = LoggerFactory.getLogger(MasterLockRepository.class);
    public static final String MASTER_LOCK_TABLE = "stb_master_locker";
    private final DataSource _dataSource;
    private final Clock _clock;

    public MasterLockRepository(DataSource dataSource, Clock clock) {
        _dataSource = dataSource;
        _clock = clock;

        // Make sure we have a table to use, and that it has the specification we expects it to have.
        validateTableVersion();
        validateTableStructure();
    }

    /**
     * Will create a master lock if it does not exists. The lock will be created with the lockTakenTime and
     * lockLastUpdateTime to {@link Instant#EPOCH} so all nodes can try to acquire it.
     * @param lockName - Name of the lock to be inserted.
     * @param nodeName - NodeName of the host currently having the lock.
     * @return - Amount of rows inserted. 1 if insert where successful. 0 if it already exists
     */
    @SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    boolean tryCreateLock(String lockName, String nodeName) {
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

    /**
     * Tries to acquire the master lock.
     * If this manages to update the lock_name it will mean this host has the lock for 5 minutes, during those 5 minutes
     * it should try to run {@link #keepLock(String, String)} so it kan keep it for as long as it needs.
     *
     * @param lockName - Name of the lock that it should attempt to aquire.
     * @param nodeName - Host name of the server that should keep the lock.
     * @return - 1 if the lock where aquired. 0 if it did not manage to aquire the lock.
     */
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

    /**
     * Allows for the node that currenly has the lock to release it by setting it to {@link Instant#EPOCH}
     * @param lockName - Lock name to release
     * @param nodeName - The node name that is the current master node.
     */
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

    /**
     * Used for the running host to keep the lock for 5 more minutes.
     * If the <b>lock_last_updated_time</b> is updated that means this host still has this master lock for another 5 minutes.
     * After 5 minutes it means no-one has it until 10 minutes has passed. At that time it is up for grabs again.
     *
     * @param lockName - name of the master lock
     * @param nodeName - host name that currently has the lock. Should be this running host.
     * @return - true if any node is updated. false if we did not manage to keep the lock (we lost the master lock).
     */
    @SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    public boolean keepLock(String lockName, String nodeName) {
        String sql = "UPDATE " + MASTER_LOCK_TABLE
                + " SET node_name = ?,lock_last_updated_time = ? "
                + " WHERE lock_name = ? "
                + " AND node_name = ? "
                // (lockLastUpdated >= $now - 5 minutes)). Can only do keeplock withing 5 min after it where last updated.
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

    /**
     * Helper method to get all the locks in the masterLock table.
     * @return
     */
    @SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    public List<MasterLockDto> getLocks() throws SQLException {
        String sql = "SELECT * FROM " + MASTER_LOCK_TABLE;

        try (Connection sqlConnection = _dataSource.getConnection();
            PreparedStatement pStmt = sqlConnection.prepareStatement(sql);
             ResultSet result = pStmt.executeQuery()) {

            List<MasterLockDbo> masterLocks = new ArrayList<>();
            while (result.next()) {
                MasterLockDbo row = new MasterLockDbo(
                        result.getString("lock_name"),
                        result.getString("node_name"),
                        result.getTimestamp("lock_taken_time"),
                        result.getTimestamp("lock_last_updated_time"));
                masterLocks.add(row);
            }
            return masterLocks.stream().map(dbo -> MasterLockDto.fromDbo(dbo)).collect(toList());
         }
    }

    /**
     * Helper method to get a specific the lock in the masterLock table.
     * @return
     */
    @SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    public Optional<MasterLockDto> getLock(String lockName) {
        String sql = "SELECT * FROM " + MASTER_LOCK_TABLE
                + " WHERE lock_name = ? ";
        try (Connection sqlConnection = _dataSource.getConnection();
             PreparedStatement pStmt = sqlConnection.prepareStatement(sql)) {
            pStmt.setString(1, lockName);
            try (ResultSet result = pStmt.executeQuery()) {
                // ?: Did we find any row?
                if (result.first()) {
                    // -> Yes we found the first row
                    MasterLockDbo dbo = new MasterLockDbo(
                            result.getString("lock_name"),
                            result.getString("node_name"),
                            result.getTimestamp("lock_taken_time"),
                            result.getTimestamp("lock_last_updated_time"));
                    return Optional.of(MasterLockDto.fromDbo(dbo));
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

    // ===== DBO ==============================================================================

    static class MasterLockDbo {
        private final String lockName;
        private final String nodeName;
        private final Timestamp lockTakenTime;
        private final Timestamp lockLastUpdatedTime;

        MasterLockDbo(String lockName, String nodeName, Timestamp lockTakenTime, Timestamp lockLastUpdatedTime) {
            this.lockName = lockName;
            this.nodeName = nodeName;
            this.lockTakenTime = lockTakenTime;
            this.lockLastUpdatedTime = lockLastUpdatedTime;
        }

        public String getLockName() {
            return lockName;
        }

        public String getNodeName() {
            return nodeName;
        }

        public LocalDateTime getLockTakenTime() {
            return lockTakenTime.toLocalDateTime();
        }

        public LocalDateTime getLockLastUpdatedTime() {
            return lockLastUpdatedTime.toLocalDateTime();
        }
    }
}
