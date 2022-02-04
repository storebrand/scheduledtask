package com.storebrand.scheduledtask;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Optional;

import javax.sql.DataSource;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;

import com.storebrand.scheduledtask.ScheduledTaskServiceImpl.MasterLockDto;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Tests for {@link MasterLockRepository}
 *
 * @author Dag Bertelsen - dag.lennart.bertelsen@storebrand.no - dabe@dagbertelsen.com - 2021.03
 */
public class MasterLockRepositoryTest {
    private final JdbcTemplate _jdbcTemplate;
    private final DataSource _dataSource;
    private final ClockMock _clock = new ClockMock();
    static final String MASTER_TABLE_CREATE_SQL = "CREATE TABLE " + MasterLockRepository.MASTER_LOCK_TABLE + " ( "
    + " lock_name VARCHAR NOT NULL, "
    + " node_name VARCHAR NOT NULL, "
    + " lock_taken_time datetime2 NOT NULL, "
    + " lock_last_updated_time datetime2 NOT NULL, "
    + " CONSTRAINT PK_lock_name PRIMARY KEY (lock_name) "
    + " );";

    static final String SCHEDULE_TABLE_VERSION_CREATE_SQL = "CREATE TABLE " + TableInspector.TABLE_VERSION + " ( "
            + " version int NOT NULL"
            + " ) ";


    public MasterLockRepositoryTest() {
        _dataSource = new SingleConnectionDataSource("jdbc:h2:mem:testMasterLockerDb", true);
        _jdbcTemplate = new JdbcTemplate(_dataSource);
    }

    @BeforeEach
    public void before() {
        _jdbcTemplate.execute(MASTER_TABLE_CREATE_SQL);
        _jdbcTemplate.execute(SCHEDULE_TABLE_VERSION_CREATE_SQL);
        _jdbcTemplate.execute("INSERT INTO " + TableInspector.TABLE_VERSION + " (version) "
                + " VALUES (" + TableInspector.VALID_VERSION + ")");
    }

    @AfterEach
    public void after() {
        _jdbcTemplate.execute("DROP TABLE " + MasterLockRepository.MASTER_LOCK_TABLE + ";");
        _jdbcTemplate.execute("DROP TABLE " + TableInspector.TABLE_VERSION + ";");

    }

    @Test
    public void createLockThatDoesNotExists() {
        // :: Setup
        MasterLockRepository master = new MasterLockRepository(_dataSource, _clock);
        // :: Act
        boolean isInserted = master.tryCreateLock("new-lock-does-not-exists", "test-node-1");

        // :: Assert
        assertTrue(isInserted);
        Optional<MasterLockDto> currentLock = master.getLock("new-lock-does-not-exists");
        assertTrue(currentLock.isPresent());
        assertEquals("test-node-1", currentLock.get().getNodeName());
    }


    @Test
    public void createTwoLockThatNotExists() {
        // :: Setup
        MasterLockRepository master = new MasterLockRepository(_dataSource, _clock);
        // :: Act
        boolean isInserted1 = master.tryCreateLock("new-lock-does-not-exists", "test-node-1");
        boolean isInserted2 = master.tryCreateLock("another-does-not-exists", "test-node-1");

        // :: Assert
        assertTrue(isInserted1);
        assertTrue(isInserted2);
        Optional<MasterLockDto> firstLock = master.getLock("new-lock-does-not-exists");
        assertTrue(firstLock.isPresent());
        assertEquals("test-node-1", firstLock.get().getNodeName());
        Optional<MasterLockDto> secondLock = master.getLock("another-does-not-exists");
        assertTrue(secondLock.isPresent());
        assertEquals("test-node-1", secondLock.get().getNodeName());
    }

    @Test
    public void createLockThatAlreadyExists() {
        // :: Setup
        MasterLockRepository master = new MasterLockRepository(_dataSource, _clock);
        boolean isInsertedInitially = master.tryCreateLock("exiting lock", "test-node-1");

        // :: Act
        boolean isInsertedSecond = master.tryCreateLock("exiting lock", "test-node-1");

        // :: Assert
        assertTrue(isInsertedInitially);
        assertFalse(isInsertedSecond);
    }

    // ===== Acquire lock =========================================================================
    @Test
    public void acquireLockThatIsNotYetAcquired_fail() {
        // :: Setup
        MasterLockRepository master = new MasterLockRepository(_dataSource, _clock);

        // :: Act
        boolean inserted = master.tryAcquireLock("unaquiredLock", "test-node-1");

        // :: Assert
        assertFalse(inserted);
        Optional<MasterLockDto> currentLock = master.getLock("unaquiredLock");
        assertFalse(currentLock.isPresent());
    }

    @Test
    public void acquireLockThatIsTaken_fail() {
        // :: Setup
        MasterLockRepository master = new MasterLockRepository(_dataSource, _clock);
        master.tryCreateLock("acquiredLock", "test-node-1");
        boolean acquired1 = master.tryAcquireLock("acquiredLock", "test-node-1");

        // :: Act
        boolean acquired2 = master.tryAcquireLock("acquiredLock", "test-node-2");

        // :: Assert
        assertTrue(acquired1);
        // Node 2 should not be able to acquire the lock due to node 1 just took this one.
        assertFalse(acquired2);
        Optional<MasterLockDto> currentLock = master.getLock("acquiredLock");
        assertTrue(currentLock.isPresent());
        assertEquals("test-node-1", currentLock.get().getNodeName());
    }

    /**
     * Neither of the nodes should be able to acquire a lock if it where taken withing the last 5 min
     */
    @Test
    public void acquireLockShouldNotBeRequired_fail() {
        // :: Setup
        LocalDateTime initiallyAcquired = LocalDateTime.of(2021, 2, 28, 13, 20);
        _clock.setFixedClock(initiallyAcquired);
        MasterLockRepository master = new MasterLockRepository(_dataSource, _clock);
        master.tryCreateLock("acquiredLock", "test-node-1");
        master.tryAcquireLock("acquiredLock", "test-node-1");
        _clock.setFixedClock(LocalDateTime.of(2021, 2, 28, 13, 22));

        // :: Act
        boolean inserted1 = master.tryAcquireLock("acquiredLock", "test-node-1");
        boolean inserted2 = master.tryAcquireLock("acquiredLock", "test-node-2");

        // :: Assert
        assertFalse(inserted1);
        // Node 2 should not be able to acquire the lock due to node 1 just took this one.
        assertFalse(inserted2);
        Optional<MasterLockDto> currentLock = master.getLock("acquiredLock");
        assertTrue(currentLock.isPresent());
        assertEquals("test-node-1", currentLock.get().getNodeName());
        assertEquals(initiallyAcquired, currentLock.get().getLockLastUpdatedTime());
        assertEquals(initiallyAcquired, currentLock.get().getLockTakenTime());
    }

    /**
     * Test for a lock that where taken 6 min ago (1 min more than the time span that the lock can still be kept for
     * the master node). Here no one should be able to take the lock due to it where last kept 5+ minutes ago.
     */
    @Test
    public void acquireLockThatIsTaken6MinAgo_fail() {
        // :: Setup
        LocalDateTime initiallyAcquired = LocalDateTime.of(2021, 2, 28, 13, 20);
        _clock.setFixedClock(initiallyAcquired);
        MasterLockRepository master = new MasterLockRepository(_dataSource, _clock);
        master.tryCreateLock("acquiredLock", "test-node-1");
        master.tryAcquireLock("acquiredLock", "test-node-1");
        // Move the clock 6 minutes
        _clock.setFixedClock(LocalDateTime.of(2021, 2, 28, 13, 26));


        // :: Act
        boolean inserted1 = master.tryAcquireLock("acquiredLock", "test-node-1");
        boolean inserted2 = master.tryAcquireLock("acquiredLock", "test-node-2");

        // :: Assert
        assertFalse(inserted1);
        // Node 2 should not be able to acquire the lock due to node 1 just took this one.
        assertFalse(inserted2);
        Optional<MasterLockDto> currentLock = master.getLock("acquiredLock");
        assertTrue(currentLock.isPresent());
        assertEquals("test-node-1", currentLock.get().getNodeName());
        assertEquals(initiallyAcquired, currentLock.get().getLockLastUpdatedTime());
        assertEquals(initiallyAcquired, currentLock.get().getLockTakenTime());
    }

    /**
     * Test for a lock that where taken 11 min ago (1 min longer than where it can be retaken by another node again)
     * After the 10 min we verify that one node manages to acquire the lock.
     */
    @Test
    public void acquireLockThatIsTaken10MinAgo_ok() {
        // :: Setup
        LocalDateTime initiallyAcquired = LocalDateTime.of(2021, 2, 28, 13, 20);
        _clock.setFixedClock(initiallyAcquired);
        MasterLockRepository master = new MasterLockRepository(_dataSource, _clock);
        master.tryCreateLock("acquiredLock", "test-node-1");
        // Move the clock 6 minutes
        LocalDateTime secondAcquire = LocalDateTime.of(2021, 2, 28, 13, 31);
        _clock.setFixedClock(secondAcquire);


        // :: Act
        // Node wins the insert this round
        boolean inserted2 = master.tryAcquireLock("acquiredLock", "test-node-2");
        boolean inserted1 = master.tryAcquireLock("acquiredLock", "test-node-1");

        // :: Assert
        assertFalse(inserted1);
        assertTrue(inserted2);
        Optional<MasterLockDto> currentLock = master.getLock("acquiredLock");
        assertTrue(currentLock.isPresent());
        assertEquals("test-node-2", currentLock.get().getNodeName());
        assertNotEquals(initiallyAcquired, currentLock.get().getLockLastUpdatedTime());
        assertEquals(secondAcquire, currentLock.get().getLockLastUpdatedTime());
        assertEquals(secondAcquire, currentLock.get().getLockTakenTime());
    }
    // ===== Keep lock ============================================================================

    /**
     * Node1 manages to get the lock, then do a keep lock update after 3 minutes. Node 2 should not be allowed to
     * acquire the lock during this time.
     */
    @Test
    public void keepLockAfterAcquired_ok() {
        // :: Setup
        LocalDateTime initiallyAcquired = LocalDateTime.of(2021, 2, 28, 13, 20);
        _clock.setFixedClock(initiallyAcquired);
        MasterLockRepository master = new MasterLockRepository(_dataSource, _clock);
        master.tryCreateLock("acquiredLock", "test-node-1");
        master.tryAcquireLock("acquiredLock", "test-node-1");

        // :: Act
        LocalDateTime keepLockTime = LocalDateTime.of(2021, 02, 28, 13, 23);
        _clock.setFixedClock(keepLockTime);
        boolean keepLockUpdatesNode1 = master.keepLock("acquiredLock", "test-node-1");
        boolean acquireLockNode2 = master.tryAcquireLock("acquiredLock", "test-node-2");

        // :: Assert
        assertTrue(keepLockUpdatesNode1);
        assertFalse(acquireLockNode2);
        Optional<MasterLockDto> currentLock = master.getLock("acquiredLock");
        assertTrue(currentLock.isPresent());
        assertEquals("test-node-1", currentLock.get().getNodeName());
        assertNotEquals(initiallyAcquired, currentLock.get().getLockLastUpdatedTime());
        assertEquals(keepLockTime, currentLock.get().getLockLastUpdatedTime());
        assertEquals(initiallyAcquired, currentLock.get().getLockTakenTime());
    }

    /**
     * Node that acquired the lock manages to keep it multiple time and we verify that after it has kept it for more
     * than 10 min it can not be acquired
     */
    @Test
    public void multipleKeepLockAfterAcquired_ok() {
        // :: Setup
        LocalDateTime initiallyAcquired = LocalDateTime.of(2021, 2, 28, 13, 20);
        _clock.setFixedClock(initiallyAcquired);
        MasterLockRepository master = new MasterLockRepository(_dataSource, _clock);
        master.tryCreateLock("acquiredLock", "test-node-1");
        master.tryAcquireLock("acquiredLock", "test-node-1");

        // :: Act
        _clock.setFixedClock(LocalDateTime.of(2021, 2, 28, 13, 23));
        boolean keepLockUpdatesNode1_first = master.keepLock("acquiredLock", "test-node-1");
        // 3 more minutes (initial acquire + 6 min)
        _clock.setFixedClock(LocalDateTime.of(2021, 2, 28, 13, 26));
        boolean keepLockUpdatesNode1_second = master.keepLock("acquiredLock", "test-node-1");
        // 4 more minutes (initial acquire + 11 min). Node 2 should not be able to acquire the lock even if the
        // first acquire where done 11 min ago
        LocalDateTime lastKeepLockTime = LocalDateTime.of(2021, 2, 28, 13, 31);
        _clock.setFixedClock(lastKeepLockTime);
        boolean keepLockUpdatesNode1_third = master.keepLock("acquiredLock", "test-node-1");
        boolean acquireLockNode2 = master.tryAcquireLock("acquiredLock", "test-node-2");

        // :: Assert
        assertTrue(keepLockUpdatesNode1_first);
        assertTrue(keepLockUpdatesNode1_second);
        assertTrue(keepLockUpdatesNode1_third);
        assertFalse(acquireLockNode2);
        Optional<MasterLockDto> currentLock = master.getLock("acquiredLock");
        assertTrue(currentLock.isPresent());
        assertEquals("test-node-1", currentLock.get().getNodeName());
        assertNotEquals(initiallyAcquired, currentLock.get().getLockLastUpdatedTime());
        assertEquals(lastKeepLockTime, currentLock.get().getLockLastUpdatedTime());
        assertEquals(initiallyAcquired, currentLock.get().getLockTakenTime());
    }

    /**
     * Node that acquired the lock manages to keep it multiple time and we verify that after it has kept it for more
     * than 10 min. During
     * this time we test if the node2 can also do the keepLock, it should not be able to aquire the lock due to first
     * node has done keepLock during these 10 minutes.
     */
    @Test
    public void multipleKeepLockAfterAcquiredOtherNodeShouldNotBeAllowedToDoKeepLock_ok() {
        // :: Setup
        LocalDateTime initiallyAcquired = LocalDateTime.of(2021, 2, 28, 13, 20);
        _clock.setFixedClock(initiallyAcquired);
        MasterLockRepository master = new MasterLockRepository(_dataSource, _clock);
        master.tryCreateLock("acquiredLock", "test-node-1");
        master.tryAcquireLock("acquiredLock", "test-node-1");

        // :: Act
        _clock.setFixedClock(LocalDateTime.of(2021, 2, 28, 13, 23));
        boolean keepLockUpdatesNode1_first = master.keepLock("acquiredLock", "test-node-1");
        boolean acquireLockNode2_first = master.tryAcquireLock("acquiredLock", "test-node-2");

        _clock.setFixedClock(LocalDateTime.of(2021, 2, 28, 13, 26));
        boolean keepLockUpdatesNode1_second = master.keepLock("acquiredLock", "test-node-1");
        boolean acquireLockNode2_second = master.tryAcquireLock("acquiredLock", "test-node-2");

        LocalDateTime lastKeepLockTime = LocalDateTime.of(2021, 2, 28, 13, 31);
        _clock.setFixedClock(lastKeepLockTime);
        boolean keepLockUpdatesNode1_third = master.keepLock("acquiredLock", "test-node-1");
        boolean acquireLockNode2_third = master.tryAcquireLock("acquiredLock", "test-node-2");

        // :: Assert
        assertTrue(keepLockUpdatesNode1_first);
        assertTrue(keepLockUpdatesNode1_second);
        assertTrue(keepLockUpdatesNode1_third);
        assertFalse(acquireLockNode2_first);
        assertFalse(acquireLockNode2_second);
        assertFalse(acquireLockNode2_third);
        Optional<MasterLockDto> currentLock = master.getLock("acquiredLock");
        assertTrue(currentLock.isPresent());
        assertEquals("test-node-1", currentLock.get().getNodeName());
        assertNotEquals(initiallyAcquired, currentLock.get().getLockLastUpdatedTime());
        assertEquals(lastKeepLockTime, currentLock.get().getLockLastUpdatedTime());
        assertEquals(initiallyAcquired, currentLock.get().getLockTakenTime());
    }

    /**
     * Testing the situation where the first node managed to acquire the lock but for some reason did not manage to keep
     * the lock. It should then be in "limbo" in the timespan 5 - 10 min before it are acquired again.
     */
    @Test
    public void firstNodeLoosesLock_ok() {
        // :: Setup
        LocalDateTime initiallyAcquired = LocalDateTime.of(2021, 2, 28, 13, 20);
        _clock.setFixedClock(initiallyAcquired);
        MasterLockRepository master = new MasterLockRepository(_dataSource, _clock);
        master.tryCreateLock("acquiredLock", "test-node-1");
        master.tryAcquireLock("acquiredLock", "test-node-1");

        // :: Act
        // Node1 does not manage to do keepLock within the timespan, it will fail to do keepLock at +6 min.
        // At this time the node2 also tries to acquire the lock but also fails due to the same reason.
        _clock.setFixedClock(LocalDateTime.of(2021, 2, 28, 13, 26));
        boolean keepLockUpdatesNode1_first = master.keepLock("acquiredLock", "test-node-1");
        boolean acquireLockNode2_first = master.tryAcquireLock("acquiredLock", "test-node-2");

        // we have passed the 10 min time where the node 1 tries to keep lock again and fails, while node 2 acquires
        // the lock
        LocalDateTime secondAcquireTime = LocalDateTime.of(2021, 2, 28, 13, 31);
        _clock.setFixedClock(secondAcquireTime);
        boolean keepLockUpdatesNode1_second = master.keepLock("acquiredLock", "test-node-1");
        boolean acquireLockNode2_second = master.tryAcquireLock("acquiredLock", "test-node-2");

        // :: Assert
        assertFalse(keepLockUpdatesNode1_first);
        assertFalse(keepLockUpdatesNode1_second);
        assertFalse(acquireLockNode2_first);
        assertTrue(acquireLockNode2_second);
        Optional<MasterLockDto> currentLock = master.getLock("acquiredLock");
        assertTrue(currentLock.isPresent());
        assertEquals("test-node-2", currentLock.get().getNodeName());
        assertNotEquals(initiallyAcquired, currentLock.get().getLockLastUpdatedTime());
        assertEquals(secondAcquireTime, currentLock.get().getLockLastUpdatedTime());
        assertEquals(secondAcquireTime, currentLock.get().getLockTakenTime());
    }

    /**
     * First node releases the lock, and the second node then acquires it
     */
    @Test
    public void firstNodeReleasesLock_ok() {
        // :: Setup
        LocalDateTime initiallyAcquired = LocalDateTime.of(2021, 2, 28, 13, 20);
        _clock.setFixedClock(initiallyAcquired);
        MasterLockRepository master = new MasterLockRepository(_dataSource, _clock);
        master.tryCreateLock("acquiredLock", "test-node-1");

        // :: Act
        // Node1 releases the lock so Node2 can acquire it
        LocalDateTime secondAcquireTime = LocalDateTime.of(2021, 2, 28, 13, 23);
        _clock.setFixedClock(secondAcquireTime);
        boolean releaseLockNode1 = master.releaseLock("acquiredLock", "test-node-1");
        master.getLock("acquiredLock");
        boolean acquireLockNode2 = master.tryAcquireLock("acquiredLock", "test-node-2");


        // :: Assert
        assertTrue(releaseLockNode1);
        assertTrue(acquireLockNode2);
        Optional<MasterLockDto> currentLock = master.getLock("acquiredLock");
        assertTrue(currentLock.isPresent());
        assertEquals("test-node-2", currentLock.get().getNodeName());
        assertNotEquals(initiallyAcquired, currentLock.get().getLockLastUpdatedTime());
        assertEquals(secondAcquireTime, currentLock.get().getLockLastUpdatedTime());
        assertEquals(secondAcquireTime, currentLock.get().getLockTakenTime());
    }

    @Test
    public void secondNodeShouldNotBeAllowedToReleaseNode1Lock_fail() {
        // :: Setup
        LocalDateTime initiallyAcquired = LocalDateTime.of(2021, 2, 28, 13, 20);
        _clock.setFixedClock(initiallyAcquired);
        MasterLockRepository master = new MasterLockRepository(_dataSource, _clock);
        master.tryCreateLock("acquiredLock", "test-node-1");
        master.tryAcquireLock("acquiredLock", "test-node-1");

        // :: Act
        // Node1 releases the lock so Node2 can acquire it
        LocalDateTime secondAcquireTime = LocalDateTime.of(2021, 2, 28, 13, 23);
        _clock.setFixedClock(secondAcquireTime);
        // node 1 keeps lock,
        boolean keepLockNode1 = master.keepLock("acquiredLock", "test-node-1");
        master.getLock("acquiredLock");
        // Node 2 should not be allowed to release another nodes lock
        boolean releaseAnothersLock = master.releaseLock("acquiredLock", "test-node-2");


        // :: Assert
        assertTrue(keepLockNode1);
        assertFalse(releaseAnothersLock);
        Optional<MasterLockDto> currentLock = master.getLock("acquiredLock");
        assertTrue(currentLock.isPresent());
        assertEquals("test-node-1", currentLock.get().getNodeName());
        assertNotEquals(initiallyAcquired, currentLock.get().getLockLastUpdatedTime());
        assertEquals(secondAcquireTime, currentLock.get().getLockLastUpdatedTime());
        assertEquals(initiallyAcquired, currentLock.get().getLockTakenTime());
    }

    // ===== Helpers ==============================================================================
    /** Fixed clock to replace real clock in unit/integration tests. */
    public static class ClockMock extends Clock {
        private Clock _clock = Clock.systemDefaultZone();

        @Override
        public ZoneId getZone() {
            return _clock.getZone();
        }

        @Override
        @SuppressFBWarnings(value = "RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT",
                justification = "Intentional to return this clock")
        public Clock withZone(ZoneId zone) {
            _clock.withZone(zone);
            return this;
        }

        @Override
        public Instant instant() {
            return _clock.instant();
        }

        public void setClock(Clock clock) {
            _clock = clock;
        }

        public void setFixedClock(LocalDateTime dateTime) {
            setClock(Clock.fixed(dateTime.atZone(ZoneId.systemDefault()).toInstant(), ZoneId.systemDefault()));
        }
    }
}