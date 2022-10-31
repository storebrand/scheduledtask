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

import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import com.storebrand.scheduledtask.ScheduledTaskRegistry.MasterLock;

/**
 * Simple in-memory implementation for {@link MasterLockRepository}. This is only intended for use in unit test, and
 * similar testing situations, where we don't want or need to spin up an actual database.
 * <p>
 * Important notice! Locking mechanism that normally ensures only one node runs a scheduled task at a time will not work
 * for this implementation, as there is no communication between service instances. All data is stored in local memory,
 * and is lost when the service stops.
 *
 * @author Kristian Hiim
 */
public class InMemoryMasterLockRepository implements MasterLockRepository {

    private final Object _lockObject = new Object();
    private final ConcurrentHashMap<String, InMemoryMasterLock> _locks = new ConcurrentHashMap<>();
    private final Clock _clock;

    public InMemoryMasterLockRepository(Clock clock) {
        _clock = clock;
    }

    @Override
    public boolean tryCreateLock(String lockName, String nodeName) {
        synchronized (_lockObject) {
            if (_locks.containsKey(lockName)) {
                return false;
            }
            InMemoryMasterLock lock = new InMemoryMasterLock(lockName, nodeName, Instant.EPOCH, Instant.EPOCH);
            _locks.put(lockName, lock);
            return true;
        }
    }

    @Override
    public boolean tryCreateMissingLock(String lockName) {
        synchronized (_lockObject) {
            if (_locks.containsKey(lockName)) {
                return false;
            }
            Instant now = Instant.now(_clock);
            InMemoryMasterLock lock = new InMemoryMasterLock(lockName, "NON-EXISTING", now, now);
            _locks.put(lockName, lock);
            return true;
        }
    }

    @Override
    public boolean tryAcquireLock(String lockName, String nodeName) {
        synchronized (_lockObject) {
            InMemoryMasterLock existingLock = _locks.get(lockName);
            if (existingLock == null) {
                return false;
            }
            Instant now = _clock.instant();
            // We should only allow to acquire the lock if the last_updated_time is older than 10 minutes.
            // Then it means it is up for grabs.
            Instant lockShouldBeOlderThan = now.minus(10, ChronoUnit.MINUTES);

            // Is the lock less than 10 minutes old?
            if (existingLock.getLockLastUpdatedTime().isAfter(lockShouldBeOlderThan)) {
                // -> Yes, return false.
                return false;
            }

            InMemoryMasterLock newLock = new InMemoryMasterLock(lockName, nodeName, now, now);
            _locks.put(lockName, newLock);
            return true;
        }
    }

    @Override
    public boolean releaseLock(String lockName, String nodeName) {
        synchronized (_lockObject) {
            InMemoryMasterLock lock = _locks.get(lockName);
            if (lock == null) {
                return false;
            }
            if (lock.getLockName().equals(lockName) && lock.getNodeName().equals(nodeName)) {
                _locks.put(lockName, lock.releaseLock());
                return true;
            }
            return false;
        }
    }

    @Override
    public boolean keepLock(String lockName, String nodeName) {
        synchronized (_lockObject) {
            InMemoryMasterLock lock = _locks.get(lockName);
            if (lock == null) {
                return false;
            }
            Instant now = _clock.instant();
            Instant lockShouldBeNewerThan = now.minus(5, ChronoUnit.MINUTES);

            if (lock.getLockLastUpdatedTime().isBefore(lockShouldBeNewerThan)) {
                return false;
            }

            if (lock.getLockName().equals(lockName) && lock.getNodeName().equals(nodeName)) {
                _locks.put(lockName, lock.lockLastUpdatedTime(now));
                return true;
            }
            return false;
        }
    }

    @Override
    public List<MasterLock> getLocks() {
        return new ArrayList<>(_locks.values());
    }

    @Override
    public Optional<MasterLock> getLock(String lockName) {
        return Optional.ofNullable(_locks.get(lockName));
    }

    private static class InMemoryMasterLock implements MasterLock {

        private final String _lockName;
        private final String _nodeName;
        private final Instant _lockTakenTime;
        private final Instant _lockLastUpdatedTime;

        private InMemoryMasterLock(String lockName, String nodeName, Instant lockTakenTime,
                Instant lockLastUpdatedTime) {
            _lockName = lockName;
            _nodeName = nodeName;
            _lockTakenTime = lockTakenTime;
            _lockLastUpdatedTime = lockLastUpdatedTime;
        }

        @Override
        public String getLockName() {
            return _lockName;
        }

        @Override
        public String getNodeName() {
            return _nodeName;
        }

        @Override
        public Instant getLockTakenTime() {
            return _lockTakenTime;
        }

        InMemoryMasterLock releaseLock() {
            return new InMemoryMasterLock(_lockName, _nodeName, Instant.EPOCH, Instant.EPOCH);
        }

        @Override
        public Instant getLockLastUpdatedTime() {
            return _lockLastUpdatedTime;
        }

        InMemoryMasterLock lockLastUpdatedTime(Instant lastUpdatedTime) {
            return new InMemoryMasterLock(_lockName, _nodeName, _lockTakenTime, lastUpdatedTime);
        }

        @Override
        public boolean isValid(Instant now) {
            return _lockLastUpdatedTime.isAfter(now.minus(5, ChronoUnit.MINUTES));
        }
    }
}
