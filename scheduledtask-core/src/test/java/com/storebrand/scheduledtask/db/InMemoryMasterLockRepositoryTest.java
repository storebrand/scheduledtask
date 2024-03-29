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

package com.storebrand.scheduledtask.db;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import com.storebrand.scheduledtask.ScheduledTaskRegistry.MasterLock;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Tests for {@link InMemoryMasterLockRepositoryTest}
 *
 * @author Dag Bertelsen - dag.lennart.bertelsen@storebrand.no - dabe@dagbertelsen.com - 2021.03
 * @author Kristian Hiim
 */
public class InMemoryMasterLockRepositoryTest {

    private final ClockMock _clock = new ClockMock();

    @Test
    public void createLockThatDoesNotExists() {
        // :: Setup
        InMemoryMasterLockRepository master = new InMemoryMasterLockRepository(_clock);
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
        InMemoryMasterLockRepository master = new InMemoryMasterLockRepository(_clock);
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
        InMemoryMasterLockRepository master = new InMemoryMasterLockRepository(_clock);
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
        MasterLockRepository master = new InMemoryMasterLockRepository(_clock);

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
        InMemoryMasterLockRepository master = new InMemoryMasterLockRepository(_clock);
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
     * Neither of the nodes should be able to acquire a lock if it where taken withing the last 5 min
     */
    @Test
    public void acquireLockShouldNotBeRequired_fail() {
        // :: Setup
        Instant initiallyAcquired = LocalDateTime.of(2021, 2, 28, 13, 20)
                .atZone(ZoneId.systemDefault()).toInstant();
        _clock.setFixedClock(initiallyAcquired);
        InMemoryMasterLockRepository master = new InMemoryMasterLockRepository(_clock);
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
     * Test for a lock that where taken 6 min ago (1 min more than the time span that the lock can still be kept for
     * the master node). Here no one should be able to take the lock due to it where last kept 5+ minutes ago.
     */
    @Test
    public void acquireLockThatIsTaken6MinAgo_fail() {
        // :: Setup
        Instant initiallyAcquired = LocalDateTime.of(2021, 2, 28, 13, 20)
                .atZone(ZoneId.systemDefault()).toInstant();
        _clock.setFixedClock(initiallyAcquired);
        InMemoryMasterLockRepository master = new InMemoryMasterLockRepository(_clock);
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
        InMemoryMasterLockRepository master = new InMemoryMasterLockRepository(_clock);
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
    // ===== Keep lock ============================================================================

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
        InMemoryMasterLockRepository master = new InMemoryMasterLockRepository(_clock);
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
        InMemoryMasterLockRepository master = new InMemoryMasterLockRepository(_clock);
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
     * Node that acquired the lock manages to keep it multiple time and we verify that after it has kept it for more
     * than 10 min. During
     * this time we test if the node2 can also do the keepLock, it should not be able to aquire the lock due to first
     * node has done keepLock during these 10 minutes.
     */
    @Test
    public void multipleKeepLockAfterAcquiredOtherNodeShouldNotBeAllowedToDoKeepLock_ok() {
        // :: Setup
        Instant initiallyAcquired = LocalDateTime.of(2021, 2, 28, 13, 20)
                .atZone(ZoneId.systemDefault()).toInstant();
        _clock.setFixedClock(initiallyAcquired);
        InMemoryMasterLockRepository master = new InMemoryMasterLockRepository(_clock);
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
        InMemoryMasterLockRepository master = new InMemoryMasterLockRepository(_clock);
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
    @SuppressFBWarnings(value = "RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT",
            justification = "We don't need to check values in the setup of the test")
    public void firstNodeReleasesLock_ok() {
        // :: Setup
        Instant initiallyAcquired = LocalDateTime.of(2021, 2, 28, 13, 20)
                .atZone(ZoneId.systemDefault()).toInstant();
        _clock.setFixedClock(initiallyAcquired);
        InMemoryMasterLockRepository master = new InMemoryMasterLockRepository(_clock);
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
    @SuppressFBWarnings(value = "RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT",
            justification = "We don't need to check values in the setup of the test")
    public void secondNodeShouldNotBeAllowedToReleaseNode1Lock_fail() {
        // :: Setup
        Instant initiallyAcquired = LocalDateTime.of(2021, 2, 28, 13, 20)
                .atZone(ZoneId.systemDefault()).toInstant();
        _clock.setFixedClock(initiallyAcquired);
        InMemoryMasterLockRepository master = new InMemoryMasterLockRepository(_clock);
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