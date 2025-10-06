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


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Optional;

import javax.sql.DataSource;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;

import com.storebrand.scheduledtask.ScheduledTaskRegistry.MasterLock;
import com.storebrand.scheduledtask.db.MasterLockRepository;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Tests for {@link MasterLockSqlRepository}
 *
 * @author Dag Bertelsen - dag.lennart.bertelsen@storebrand.no - dabe@dagbertelsen.com - 2021.03
 */
public class MasterLockRepositoryTest {
    private static final Logger log = LoggerFactory.getLogger(MasterLockRepositoryTest.class);
    private final JdbcTemplate _jdbcTemplate;
    private final DataSource _dataSource;
    private final ClockMock _clock = new ClockMock();
    static final String MASTER_TABLE_CREATE_SQL = "CREATE TABLE " + MasterLockSqlRepository.MASTER_LOCK_TABLE + " ( "
            + " lock_name VARCHAR(255) NOT NULL, "
            + " node_name VARCHAR(255) NOT NULL, "
            + " lock_taken_time_utc DATETIME2 NOT NULL, "
            + " lock_last_updated_time_utc DATETIME2 NOT NULL, "
            + " CONSTRAINT PK_lock_name PRIMARY KEY (lock_name) "
            + " );";

    static final String SCHEDULE_TABLE_VERSION_CREATE_SQL = "CREATE TABLE " + TableInspector.TABLE_VERSION + " ( "
            + " version INT NOT NULL"
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
        _jdbcTemplate.execute("DROP TABLE " + MasterLockSqlRepository.MASTER_LOCK_TABLE + ";");
        _jdbcTemplate.execute("DROP TABLE " + TableInspector.TABLE_VERSION + ";");

    }

    @Test
    public void createLockThatDoesNotExists() {
        // :: Setup
        MasterLockSqlRepository master = new MasterLockSqlRepository(_dataSource, _clock);
        // :: Act
        boolean isInserted = master.tryCreateLock("new-lock-does-not-exists", "test-node-1");

        // :: Assert
        assertTrue(isInserted);
        Optional<MasterLock> currentLock = master.getLock("new-lock-does-not-exists");
        assertTrue(currentLock.isPresent());
        assertEquals("test-node-1", currentLock.get().getNodeName());
    }


    @Test
    public void createTwoLockThatNotExists() {
        // :: Setup
        MasterLockSqlRepository master = new MasterLockSqlRepository(_dataSource, _clock);
        // :: Act
        boolean isInserted1 = master.tryCreateLock("new-lock-does-not-exists", "test-node-1");
        boolean isInserted2 = master.tryCreateLock("another-does-not-exists", "test-node-1");

        // :: Assert
        assertTrue(isInserted1);
        assertTrue(isInserted2);
        Optional<MasterLock> firstLock = master.getLock("new-lock-does-not-exists");
        assertTrue(firstLock.isPresent());
        assertEquals("test-node-1", firstLock.get().getNodeName());
        Optional<MasterLock> secondLock = master.getLock("another-does-not-exists");
        assertTrue(secondLock.isPresent());
        assertEquals("test-node-1", secondLock.get().getNodeName());
    }

    @Test
    public void createLockThatAlreadyExists() {
        // :: Setup
        MasterLockSqlRepository master = new MasterLockSqlRepository(_dataSource, _clock);
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
        MasterLockRepository master = new MasterLockSqlRepository(_dataSource, _clock);

        // :: Act
        boolean inserted = master.tryAcquireLock("unaquiredLock", "test-node-1");

        // :: Assert
        assertFalse(inserted);
        Optional<MasterLock> currentLock = master.getLock("unaquiredLock");
        assertFalse(currentLock.isPresent());
    }

    @Test
    public void acquireLockThatIsTaken_fail() {
        // :: Setup
        MasterLockSqlRepository master = new MasterLockSqlRepository(_dataSource, _clock);
        master.tryCreateLock("acquiredLock", "test-node-1");
        boolean acquired1 = master.tryAcquireLock("acquiredLock", "test-node-1");

        // :: Act
        boolean acquired2 = master.tryAcquireLock("acquiredLock", "test-node-2");

        // :: Assert
        assertTrue(acquired1);
        // Node 2 should not be able to acquire the lock due to node 1 just took this one.
        assertFalse(acquired2);
        Optional<MasterLock> currentLock = master.getLock("acquiredLock");
        assertTrue(currentLock.isPresent());
        assertEquals("test-node-1", currentLock.get().getNodeName());
    }

    /**
     * Testing release lock when the lock is not acquired.
     */
    @Test
    public void releaseLock_ok() {
        // :: Setup
        MasterLockSqlRepository master = new MasterLockSqlRepository(_dataSource, _clock);
        master.tryCreateLock("acquiredLock", "test-node-1");
        boolean acquired1 = master.tryAcquireLock("acquiredLock", "test-node-1");

        // :: Act
        boolean releaseLock = master.releaseLock("acquiredLock", "test-node-1");

        // :: Assert
        assertTrue(acquired1);
        assertTrue(releaseLock);
        Optional<MasterLock> currentLock = master.getLock("acquiredLock");
        assertTrue(currentLock.isPresent());
        assertEquals("test-node-1", currentLock.get().getNodeName());
        assertEquals(Instant.EPOCH, currentLock.get().getLockTakenTime());
    }

    /**
     * Neither of the nodes should be able to acquire a lock if it where taken withing the last 5 min
     */
    @Test
    public void acquireLockShouldNotBeRequired_fail() {
        // :: Setup
        Instant initiallyAcquired = LocalDateTime.of(2021, 2, 28, 13, 20)
                .atZone(ZoneId.systemDefault()).toInstant();
        _clock.setFixedClock(initiallyAcquired);
        MasterLockSqlRepository master = new MasterLockSqlRepository(_dataSource, _clock);
        master.tryCreateLock("acquiredLock", "test-node-1");
        master.tryAcquireLock("acquiredLock", "test-node-1");
        _clock.setFixedClock(LocalDateTime.of(2021, 2, 28, 13, 22)
                .atZone(ZoneId.systemDefault()).toInstant());

        // :: Act
        boolean inserted1 = master.tryAcquireLock("acquiredLock", "test-node-1");
        boolean inserted2 = master.tryAcquireLock("acquiredLock", "test-node-2");

        // :: Assert
        assertFalse(inserted1);
        // Node 2 should not be able to acquire the lock due to node 1 just took this one.
        assertFalse(inserted2);
        Optional<MasterLock> currentLock = master.getLock("acquiredLock");
        assertTrue(currentLock.isPresent());
        assertEquals("test-node-1", currentLock.get().getNodeName());
        assertEquals(initiallyAcquired, currentLock.get().getLockLastUpdatedTime());
        assertEquals(initiallyAcquired, currentLock.get().getLockTakenTime());
    }

    /**
     * Test for a lock that where taken 6 min ago (1 min more than the time span that the lock can still be kept for the
     * master node). Here no one should be able to take the lock due to it where last kept 5+ minutes ago.
     */
    @Test
    public void acquireLockThatIsTaken6MinAgo_fail() {
        // :: Setup
        Instant initiallyAcquired = LocalDateTime.of(2021, 2, 28, 13, 20)
                .atZone(ZoneId.systemDefault()).toInstant();
        _clock.setFixedClock(initiallyAcquired);
        MasterLockSqlRepository master = new MasterLockSqlRepository(_dataSource, _clock);
        master.tryCreateLock("acquiredLock", "test-node-1");
        master.tryAcquireLock("acquiredLock", "test-node-1");
        // Move the clock 6 minutes
        _clock.setFixedClock(LocalDateTime.of(2021, 2, 28, 13, 26)
                .atZone(ZoneId.systemDefault()).toInstant());


        // :: Act
        boolean inserted1 = master.tryAcquireLock("acquiredLock", "test-node-1");
        boolean inserted2 = master.tryAcquireLock("acquiredLock", "test-node-2");

        // :: Assert
        assertFalse(inserted1);
        // Node 2 should not be able to acquire the lock due to node 1 just took this one.
        assertFalse(inserted2);
        Optional<MasterLock> currentLock = master.getLock("acquiredLock");
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
        Instant initiallyAcquired = LocalDateTime.of(2021, 2, 28, 13, 20)
                .atZone(ZoneId.systemDefault()).toInstant();
        _clock.setFixedClock(initiallyAcquired);
        MasterLockSqlRepository master = new MasterLockSqlRepository(_dataSource, _clock);
        master.tryCreateLock("acquiredLock", "test-node-1");
        // Move the clock 6 minutes
        Instant secondAcquire = LocalDateTime.of(2021, 2, 28, 13, 31)
                .atZone(ZoneId.systemDefault()).toInstant();
        _clock.setFixedClock(secondAcquire);


        // :: Act
        // Node wins the insert this round
        boolean inserted2 = master.tryAcquireLock("acquiredLock", "test-node-2");
        boolean inserted1 = master.tryAcquireLock("acquiredLock", "test-node-1");

        // :: Assert
        assertFalse(inserted1);
        assertTrue(inserted2);
        Optional<MasterLock> currentLock = master.getLock("acquiredLock");
        assertTrue(currentLock.isPresent());
        assertEquals("test-node-2", currentLock.get().getNodeName());
        assertNotEquals(initiallyAcquired, currentLock.get().getLockLastUpdatedTime());
        assertEquals(secondAcquire, currentLock.get().getLockLastUpdatedTime());
        assertEquals(secondAcquire, currentLock.get().getLockTakenTime());
    }

    /**
     * Test for daylight-saving time. Since the code is adjusting the times by subtracting 10/5 minutes from the current
     * time, we should check that the time is not affected by the daylight-saving time. When we subtract 5 minutes from
     * a time that just passed daylight-saving time the time should be 1 hour earlier than the current time (depending
     * on the zone the code uses).
     */
    @Test
    public void from_dayLightSaving_should_not_affect_acquireLock_ok() {
        // :: Setup
        Instant initiallyAcquired = Instant.parse("2024-10-27T01:02:28.586Z");
        _clock.setFixedClock(initiallyAcquired);
        MasterLockSqlRepository master = new MasterLockSqlRepository(_dataSource, _clock);
        master.tryCreateLock("acquiredLock", "test-node-1");

        // :: Act
        // first node acquires the lock
        boolean inserted1 = master.tryAcquireLock("acquiredLock", "test-node-1");
        // move the clock 2 minutes ahead, this should not affect the lock time even if we are in daylight-saving time
        _clock.setFixedClock(Instant.parse("2024-10-27T01:04:28.586Z"));
        boolean inserted2 = master.tryAcquireLock("acquiredLock", "test-node-2");

        // :: Assert
        // First node should be able to acquire the lock.
        assertTrue(inserted1);
        // Node 2 should not be able to acquire the lock due to node 1 just took this one.
        assertFalse(inserted2);

        Optional<MasterLock> currentLock = master.getLock("acquiredLock");
        assertTrue(currentLock.isPresent());
        assertEquals("test-node-1", currentLock.get().getNodeName());
        assertEquals(initiallyAcquired, currentLock.get().getLockLastUpdatedTime());
        assertEquals(initiallyAcquired, currentLock.get().getLockTakenTime());
    }

    /**
     * It's daylight-saving-time hours (autumn), and we should still be able to take the lock. Even during the hour
     * when the clock jumps one hour back.
     */
    @Test
    public void acquire_lock_ok() {
        // :: Setup
        Instant initiallyAcquired = Instant.parse("2024-10-27T01:59:28.586Z");
        _clock.setFixedClock(initiallyAcquired);
        MasterLockSqlRepository master = new MasterLockSqlRepository(_dataSource, _clock);
        master.tryCreateLock("acquiredLock", "test-node-1");

        // :: Act
        // first node acquires the lock
        boolean inserted1 = master.tryAcquireLock("acquiredLock", "test-node-1");
        // move the clock 2 minutes ahead, this should not affect the lock time even if we are in daylight-saving time
        Instant adjustedInstant = Instant.parse("2024-10-27T02:02:28.586Z");
        _clock.setFixedClock(adjustedInstant);
        boolean kept = master.keepLock("acquiredLock", "test-node-1");

        // :: Assert
        // First node should be able to acquire the lock.
        assertTrue(inserted1);
        assertTrue(kept);

        Optional<MasterLock> currentLock = master.getLock("acquiredLock");
        assertTrue(currentLock.isPresent());
        assertEquals("test-node-1", currentLock.get().getNodeName());
        log.error("### Lock.lastUpdatedTime: " + currentLock.get().getLockLastUpdatedTime() + ", LocalDateTime: " + currentLock.get().getLockLastUpdatedTime());
        assertEquals(adjustedInstant, currentLock.get().getLockLastUpdatedTime());
        assertEquals(initiallyAcquired, currentLock.get().getLockTakenTime());
    }


    // ===== Keep lock ============================================================================

    /**
     * Node1 manages to get the lock, then do a keep lock update after 3 minutes. Node 2 should not be allowed to
     * acquire the lock during this time. This occurs during the hour when daylight-saving is adjusted to winter time.
     */
    @Test
    public void keepLockAfterAcquired_from_daylightSavingTime_ok() {
        // :: Setup
        // We use instant here due to we want to have control on what "hour" the dts is in. At this time,
        // the clock moves one our back at 3:00 so there is in LocalDateTime two 02:00 but in zulu time there is one 00:00
        // and one 01:00 that is the same time in localDateTime
        Instant initiallyAcquired = Instant.parse("2024-10-27T01:02:28.586Z");
        _clock.setFixedClock(initiallyAcquired);
        MasterLockSqlRepository master = new MasterLockSqlRepository(_dataSource, _clock);
        master.tryCreateLock("acquiredLock", "test-node-1");
        boolean acquiredLock = master.tryAcquireLock("acquiredLock", "test-node-1");

        // :: Act
        // move the clock 2 minutes ahead, this should not affect the lock time even if we are in daylight-saving time
        Instant updatedTime = Instant.parse("2024-10-27T01:04:28.586Z");
        _clock.setFixedClock(updatedTime);
        boolean keepLock = master.keepLock("acquiredLock", "test-node-1");

        // :: Assert - locks where recently acquired, so node 1 should be able to keep the lock
        assertTrue(acquiredLock);
        assertTrue(keepLock);
        Optional<MasterLock> currentLock = master.getLock("acquiredLock");
        assertTrue(currentLock.isPresent());
        assertEquals("test-node-1", currentLock.get().getNodeName());
        assertEquals(updatedTime, currentLock.get().getLockLastUpdatedTime());
        assertEquals(initiallyAcquired, currentLock.get().getLockTakenTime());
    }

    /**
     * We start at 23:00 and keep the lock for 250 minutes by 1-minute interval. This tests when we are in the
     * winter time and no daylight-saving time is adjusted. Node 2 should not be able to acquire the lock during this time.
     */
    @Test
    public void keepLockAfterAcquired_slide_wintertime() {
        keepLockSlide(Instant.parse("2024-01-01T23:00:00.000Z"));
    }

    /**
     * We start at 23:00 and keep the lock for 250 minutes by 1-minute interval. This tests it when we are in the
     * spring when we adjust the clock to daylight-saving time by moving the clock back 1 hour at 03:00 to 02:00.
     * We should be able to keep the lock in this entire period.
     * Node 2 should not be able to acquire the lock during this time.
     */
    @Test
    public void keepLockAfterAcquired_slide_springTransition() {
        // an hour before midnight 2024-03-31
        keepLockSlide(Instant.parse("2024-03-30T23:00:00.000Z"));
    }

    /**
     * We start at 23:00 and keep the lock for 250 minutes by 1-minute interval. This tests when we are in the summer
     * time and no daylight-saving time is adjusted. We should be able to keep the lock on this entire period.
     * Node 2 should not be able to acquire the lock during this time.
     */
    @Test
    public void keepLockAfterAcquired_slide_summertime() {
        keepLockSlide(Instant.parse("2024-07-01T23:00:00.000Z"));
    }

    /**
     * We start at 23:00 and keep the lock for 250 minutes by 1-minute interval. This tests it when we are in the autumn
     * transition when we adjust the clock to daylight-saving time by moving the clock back 1 hour at 03:00 to 02:00.
     * At this time, we will get the hours between 2 and 3 twice. We should be able to keep the lock on this entire period.
     * Node 2 should not be able to acquire the lock during this time.
     */
    @Test
    public void keepLockAfterAcquired_slide_autumnTransition() {
        // an hour before midnight 2024-10-27
        keepLockSlide(Instant.parse("2024-10-26T23:00:00.000Z"));
    }

    /**
     * Helper method to test the keep lock for 250 minutes by 1-minute interval.
     * Node 2 should not be able to acquire the lock during this time.
     */
    public void keepLockSlide(Instant start) {
        // :: Setup
        _clock.setFixedClock(start);
        MasterLockSqlRepository master = new MasterLockSqlRepository(_dataSource, _clock);
        boolean created = master.tryCreateLock("acquiredLock", "test-node-1");
        boolean tryAcquired = master.tryAcquireLock("acquiredLock", "test-node-1");
        boolean kept = master.keepLock("acquiredLock", "test-node-1");

        assertTrue(created, "Lock should initially be created");
        assertTrue(tryAcquired, "Lock should initially be tryAcquired");
        assertTrue(kept, "Lock should initially be kept");

        // :: Act
        Instant slide = start;
        for (int t = 0; t < 250; t++) {
            _clock.setFixedClock(slide);
            boolean wholeHour = slide.atZone(ZoneId.systemDefault()).getMinute() == 0;
            if (wholeHour) {
                log.info("\n\n!!!!!!!!! Whole hour: " + slide + ", LocalDateTime: " + LocalDateTime.ofInstant(slide, ZoneId.systemDefault()));
            }
            log.info("### Slide time: " + slide + ", LocalDateTime: " + LocalDateTime.ofInstant(slide, ZoneId.systemDefault()));
            boolean acquireLockNode2 = master.tryAcquireLock("acquiredLock", "test-node-2");
            boolean keepLockUpdatesNode1 = master.keepLock("acquiredLock", "test-node-1");
            boolean keepLockNode2 = master.keepLock("acquiredLock", "test-node-2");
            assertTrue(keepLockUpdatesNode1, "Node 1 should be able to keep the lock");
            assertFalse(acquireLockNode2, "Node 2 should not be able to acquire the lock");
            assertFalse(keepLockNode2, "Node 2 should not be able to keep the lock");

            Optional<MasterLock> currentLock = master.getLock("acquiredLock");
            log.info(".. ### Lock.lastUpdatedTime: " + currentLock.get().getLockLastUpdatedTime() + ", LocalDateTime: " + currentLock.get().getLockLastUpdatedTime());
            assertTrue(currentLock.isPresent());
            assertEquals("test-node-1", currentLock.get().getNodeName());

            assertEquals(slide, currentLock.get().getLockLastUpdatedTime());
            assertEquals(start, currentLock.get().getLockTakenTime());
            // move the slide 1 minute ahead
            slide = slide.plus(1, ChronoUnit.MINUTES);
            log.info("\n--------------------------------------\n\n");
        }
    }


    /**
     * Node1 manages to get the lock, then do a keep lock update after 3 minutes. Node 2 should not be allowed to
     * acquire the lock during this time. This occurs during the hour when wintertime is adjusted to daylight-saving.
     */
    @Test
    public void keepLockAfterAcquired_to_daylightSavingTime_ok() {
        // :: Setup
        // We use instant here due to we want to have control on what "hour" the dts is in.
        Instant initiallyAcquired = Instant.parse("2024-03-31T00:59:00Z");
        _clock.setFixedClock(initiallyAcquired);
        MasterLockSqlRepository master = new MasterLockSqlRepository(_dataSource, _clock);
        master.tryCreateLock("acquiredLock", "test-node-1");
        master.tryAcquireLock("acquiredLock", "test-node-1");

        // :: Act
        // move the clock 2 minutes ahead, this should not affect the lock time even if we are in daylight-saving time
        Instant keepLockTime = Instant.parse("2024-03-31T01:04:00Z");
        _clock.setFixedClock(keepLockTime);
        boolean keepLockUpdatesNode1 = master.keepLock("acquiredLock", "test-node-1");
        boolean acquireLockNode2 = master.tryAcquireLock("acquiredLock", "test-node-2");

        // :: Assert
        assertTrue(keepLockUpdatesNode1);
        assertFalse(acquireLockNode2);
        Optional<MasterLock> currentLock = master.getLock("acquiredLock");
        assertTrue(currentLock.isPresent());
        assertEquals("test-node-1", currentLock.get().getNodeName());
        assertNotEquals(initiallyAcquired, currentLock.get().getLockLastUpdatedTime());
        assertEquals(keepLockTime, currentLock.get().getLockLastUpdatedTime());
        assertEquals(initiallyAcquired, currentLock.get().getLockTakenTime());
    }


    /**
     * Node1 manages to get the lock, then do a keep lock update after 3 minutes. Node 2 should not be allowed to
     * acquire the lock during this time.
     */
    @Test
    public void keepLockAfterAcquired_ok() {
        // :: Setup
        Instant initiallyAcquired = LocalDateTime.of(2021, 2, 28, 13, 20)
                .atZone(ZoneId.systemDefault()).toInstant();
        _clock.setFixedClock(initiallyAcquired);
        MasterLockSqlRepository master = new MasterLockSqlRepository(_dataSource, _clock);
        master.tryCreateLock("acquiredLock", "test-node-1");
        master.tryAcquireLock("acquiredLock", "test-node-1");

        // :: Act
        Instant keepLockTime = LocalDateTime.of(2021, 02, 28, 13, 23)
                .atZone(ZoneId.systemDefault()).toInstant();
        _clock.setFixedClock(keepLockTime);
        boolean keepLockUpdatesNode1 = master.keepLock("acquiredLock", "test-node-1");
        boolean acquireLockNode2 = master.tryAcquireLock("acquiredLock", "test-node-2");

        // :: Assert
        assertTrue(keepLockUpdatesNode1);
        assertFalse(acquireLockNode2);
        Optional<MasterLock> currentLock = master.getLock("acquiredLock");
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
        Instant initiallyAcquired = LocalDateTime.of(2021, 2, 28, 13, 20)
                .atZone(ZoneId.systemDefault()).toInstant();
        _clock.setFixedClock(initiallyAcquired);
        MasterLockSqlRepository master = new MasterLockSqlRepository(_dataSource, _clock);
        master.tryCreateLock("acquiredLock", "test-node-1");
        master.tryAcquireLock("acquiredLock", "test-node-1");

        // :: Act
        _clock.setFixedClock(LocalDateTime.of(2021, 2, 28, 13, 23)
                .atZone(ZoneId.systemDefault()).toInstant());
        boolean keepLockUpdatesNode1_first = master.keepLock("acquiredLock", "test-node-1");
        // 3 more minutes (initial acquire + 6 min)
        _clock.setFixedClock(LocalDateTime.of(2021, 2, 28, 13, 26)
                .atZone(ZoneId.systemDefault()).toInstant());
        boolean keepLockUpdatesNode1_second = master.keepLock("acquiredLock", "test-node-1");
        // 4 more minutes (initial acquire + 11 min). Node 2 should not be able to acquire the lock even if the
        // first acquire where done 11 min ago
        Instant lastKeepLockTime = LocalDateTime.of(2021, 2, 28, 13, 31)
                .atZone(ZoneId.systemDefault()).toInstant();
        _clock.setFixedClock(lastKeepLockTime);
        boolean keepLockUpdatesNode1_third = master.keepLock("acquiredLock", "test-node-1");
        boolean acquireLockNode2 = master.tryAcquireLock("acquiredLock", "test-node-2");

        // :: Assert
        assertTrue(keepLockUpdatesNode1_first);
        assertTrue(keepLockUpdatesNode1_second);
        assertTrue(keepLockUpdatesNode1_third);
        assertFalse(acquireLockNode2);
        Optional<MasterLock> currentLock = master.getLock("acquiredLock");
        assertTrue(currentLock.isPresent());
        assertEquals("test-node-1", currentLock.get().getNodeName());
        assertNotEquals(initiallyAcquired, currentLock.get().getLockLastUpdatedTime());
        assertEquals(lastKeepLockTime, currentLock.get().getLockLastUpdatedTime());
        assertEquals(initiallyAcquired, currentLock.get().getLockTakenTime());
    }

    /**
     * Node that acquired the lock manages to keep it multiple time, and we verify that after it has kept it for more
     * than 10 min. During this time we test if the node2 can also do the keepLock, it should not be able to acquire the
     * lock due to the first node has done keepLock during these 10 minutes.
     */
    @Test
    public void multipleKeepLockAfterAcquiredOtherNodeShouldNotBeAllowedToDoKeepLock_ok() {
        // :: Setup
        Instant initiallyAcquired = LocalDateTime.of(2021, 2, 28, 13, 20)
                .atZone(ZoneId.systemDefault()).toInstant();
        _clock.setFixedClock(initiallyAcquired);
        MasterLockSqlRepository master = new MasterLockSqlRepository(_dataSource, _clock);
        master.tryCreateLock("acquiredLock", "test-node-1");
        master.tryAcquireLock("acquiredLock", "test-node-1");

        // :: Act
        _clock.setFixedClock(LocalDateTime.of(2021, 2, 28, 13, 23)
                .atZone(ZoneId.systemDefault()).toInstant());
        boolean keepLockUpdatesNode1_first = master.keepLock("acquiredLock", "test-node-1");
        boolean acquireLockNode2_first = master.tryAcquireLock("acquiredLock", "test-node-2");

        _clock.setFixedClock(LocalDateTime.of(2021, 2, 28, 13, 26)
                .atZone(ZoneId.systemDefault()).toInstant());
        boolean keepLockUpdatesNode1_second = master.keepLock("acquiredLock", "test-node-1");
        boolean acquireLockNode2_second = master.tryAcquireLock("acquiredLock", "test-node-2");

        Instant lastKeepLockTime = LocalDateTime.of(2021, 2, 28, 13, 31)
                .atZone(ZoneId.systemDefault()).toInstant();
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
        Optional<MasterLock> currentLock = master.getLock("acquiredLock");
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
        Instant initiallyAcquired = LocalDateTime.of(2021, 2, 28, 13, 20)
                .atZone(ZoneId.systemDefault()).toInstant();
        _clock.setFixedClock(initiallyAcquired);
        MasterLockSqlRepository master = new MasterLockSqlRepository(_dataSource, _clock);
        master.tryCreateLock("acquiredLock", "test-node-1");
        master.tryAcquireLock("acquiredLock", "test-node-1");

        // :: Act
        // Node1 does not manage to do keepLock within the timespan, it will fail to do keepLock at +6 min.
        // At this time the node2 also tries to acquire the lock but also fails due to the same reason.
        _clock.setFixedClock(LocalDateTime.of(2021, 2, 28, 13, 26)
                .atZone(ZoneId.systemDefault()).toInstant());
        boolean keepLockUpdatesNode1_first = master.keepLock("acquiredLock", "test-node-1");
        boolean acquireLockNode2_first = master.tryAcquireLock("acquiredLock", "test-node-2");

        // we have passed the 10 min time where the node 1 tries to keep lock again and fails, while node 2 acquires
        // the lock
        Instant secondAcquireTime = LocalDateTime.of(2021, 2, 28, 13, 31)
                .atZone(ZoneId.systemDefault()).toInstant();
        _clock.setFixedClock(secondAcquireTime);
        boolean keepLockUpdatesNode1_second = master.keepLock("acquiredLock", "test-node-1");
        boolean acquireLockNode2_second = master.tryAcquireLock("acquiredLock", "test-node-2");

        // :: Assert
        assertFalse(keepLockUpdatesNode1_first);
        assertFalse(keepLockUpdatesNode1_second);
        assertFalse(acquireLockNode2_first);
        assertTrue(acquireLockNode2_second);
        Optional<MasterLock> currentLock = master.getLock("acquiredLock");
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
        Instant initiallyAcquired = LocalDateTime.of(2021, 2, 28, 13, 20)
                .atZone(ZoneId.systemDefault()).toInstant();
        _clock.setFixedClock(initiallyAcquired);
        MasterLockSqlRepository master = new MasterLockSqlRepository(_dataSource, _clock);
        master.tryCreateLock("acquiredLock", "test-node-1");

        // :: Act
        // Node1 releases the lock so Node2 can acquire it
        Instant secondAcquireTime = LocalDateTime.of(2021, 2, 28, 13, 23)
                .atZone(ZoneId.systemDefault()).toInstant();
        _clock.setFixedClock(secondAcquireTime);
        boolean releaseLockNode1 = master.releaseLock("acquiredLock", "test-node-1");
        master.getLock("acquiredLock");
        boolean acquireLockNode2 = master.tryAcquireLock("acquiredLock", "test-node-2");


        // :: Assert
        assertTrue(releaseLockNode1);
        assertTrue(acquireLockNode2);
        Optional<MasterLock> currentLock = master.getLock("acquiredLock");
        assertTrue(currentLock.isPresent());
        assertEquals("test-node-2", currentLock.get().getNodeName());
        assertNotEquals(initiallyAcquired, currentLock.get().getLockLastUpdatedTime());
        assertEquals(secondAcquireTime, currentLock.get().getLockLastUpdatedTime());
        assertEquals(secondAcquireTime, currentLock.get().getLockTakenTime());
    }

    @Test
    public void secondNodeShouldNotBeAllowedToReleaseNode1Lock_fail() {
        // :: Setup
        Instant initiallyAcquired = LocalDateTime.of(2021, 2, 28, 13, 20)
                .atZone(ZoneId.systemDefault()).toInstant();
        _clock.setFixedClock(initiallyAcquired);
        MasterLockSqlRepository master = new MasterLockSqlRepository(_dataSource, _clock);
        master.tryCreateLock("acquiredLock", "test-node-1");
        master.tryAcquireLock("acquiredLock", "test-node-1");

        // :: Act
        // Node1 releases the lock so Node2 can acquire it
        Instant secondAcquireTime = LocalDateTime.of(2021, 2, 28, 13, 23)
                .atZone(ZoneId.systemDefault()).toInstant();
        _clock.setFixedClock(secondAcquireTime);
        // node 1 keeps lock,
        boolean keepLockNode1 = master.keepLock("acquiredLock", "test-node-1");
        master.getLock("acquiredLock");
        // Node 2 should not be allowed to release another nodes lock
        boolean releaseAnothersLock = master.releaseLock("acquiredLock", "test-node-2");


        // :: Assert
        assertTrue(keepLockNode1);
        assertFalse(releaseAnothersLock);
        Optional<MasterLock> currentLock = master.getLock("acquiredLock");
        assertTrue(currentLock.isPresent());
        assertEquals("test-node-1", currentLock.get().getNodeName());
        assertNotEquals(initiallyAcquired, currentLock.get().getLockLastUpdatedTime());
        assertEquals(secondAcquireTime, currentLock.get().getLockLastUpdatedTime());
        assertEquals(initiallyAcquired, currentLock.get().getLockTakenTime());
    }

    @Test
    public void acquiringLockShouldNotBePossibleBefore10MinutesAfterCreatingMissingLock() {
        // :: Setup
        Instant now = LocalDateTime.of(2022, 2, 2, 2, 2, 2)
                .atZone(ZoneId.systemDefault()).toInstant();
        _clock.setFixedClock(now);
        MasterLockSqlRepository repository = new MasterLockSqlRepository(_dataSource, _clock);

        // :: Act
        boolean created = repository.tryCreateMissingLock("test-lock");
        boolean acquiredImmediately = repository.tryAcquireLock("test-lock", "test-node-1");

        // Move 11 minutes ahead, so we should be able to acquire lock.
        _clock.setFixedClock(now.plus(11, ChronoUnit.MINUTES));
        boolean acquired11MinutesLater = repository.tryAcquireLock("test-lock", "test-node-1");

        // :: Assert
        assertTrue(created);
        assertFalse(acquiredImmediately);
        assertTrue(acquired11MinutesLater);
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

        public void setFixedClock(Instant instant) {
            setClock(Clock.fixed(instant, ZoneId.systemDefault()));
        }

        public void setFixedClock(LocalDateTime instant) {
            setClock(Clock.fixed(instant.atZone(ZoneId.systemDefault()).toInstant(), ZoneId.systemDefault()));
        }
    }
}