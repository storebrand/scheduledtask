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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.storebrand.scheduledtask.ScheduledTask.Criticality;
import com.storebrand.scheduledtask.ScheduledTask.Recovery;
import com.storebrand.scheduledtask.ScheduledTaskConfig;
import com.storebrand.scheduledtask.ScheduledTaskRegistry;
import com.storebrand.scheduledtask.ScheduledTaskRegistry.LogEntry;
import com.storebrand.scheduledtask.ScheduledTaskRegistry.RunOnce;
import com.storebrand.scheduledtask.ScheduledTaskRegistry.Schedule;
import com.storebrand.scheduledtask.db.InMemoryMasterLockRepositoryTest.ClockMock;
import com.storebrand.scheduledtask.db.ScheduledTaskRepository.ScheduledRunDto;

/**
 * Tests for {@link InMemoryScheduledTaskRepository}
 *
 * @author Dag Bertelsen - dag.lennart.bertelsen@storebrand.no - dabe@dagbertelsen.com - 2021.03
 */
public class InMemoryScheduledTaskRepositoryTest {
    private final ClockMock _clock = new ClockMock();

    @Test
    public void createScheduleThatDoesNotExists_ok() {
        // :: Setup
        InMemoryScheduledTaskRepository schedulerRep = new InMemoryScheduledTaskRepository(_clock);
        _clock.setFixedClock(LocalDateTime.of(2021, 3, 3, 12, 1));
        ScheduledTaskConfig config = new ScheduledTaskConfig(
                "testSchedule",
                "0 0 23 ? * *",
                1,
                Criticality.MINOR,
                Recovery.SELF_HEALING
        );
        // :: Act
        int created = schedulerRep.createSchedule(config);

        // :: Assert
        assertEquals(1, created);
        Optional<Schedule> schedule = schedulerRep.getSchedule("testSchedule");
        assertTrue(schedule.isPresent());
        assertEquals(getInstant(2021, 3, 3, 23, 0), schedule.get().getNextRun());
        assertTrue(schedule.get().isActive());
        assertFalse(schedule.get().isRunOnce());
    }

    @Test
    public void createScheduleThatAlreadyExists_fail() {
        // :: Setup
        InMemoryScheduledTaskRepository schedulerRep = new InMemoryScheduledTaskRepository(_clock);
        _clock.setFixedClock(getInstant(2021, 3, 3, 12, 24));

        ScheduledTaskConfig config = new ScheduledTaskConfig(
                "alreadyExists",
                "0 0 23 ? * *",
                1,
                Criticality.MINOR,
                Recovery.SELF_HEALING
        );
        int firstInsert = schedulerRep.createSchedule(config);

        // :: Act
        int secondInsert = schedulerRep.createSchedule(config);

        // :: Assert
        assertEquals(1, firstInsert);
        assertEquals(0, secondInsert);
        Optional<Schedule> schedule = schedulerRep.getSchedule("alreadyExists");
        assertTrue(schedule.isPresent());
        assertEquals(getInstant(2021, 3, 3, 23, 0), schedule.get().getNextRun());
        assertFalse(schedule.get().getOverriddenCronExpression().isPresent());
    }

    @Test
    public void updateNextRun() {
        // :: Setup
        InMemoryScheduledTaskRepository schedulerRep = new InMemoryScheduledTaskRepository(_clock);
        LocalDateTime insertTime = LocalDateTime.of(2021, 3, 3, 12, 1);
        _clock.setFixedClock(insertTime);
        ScheduledTaskConfig config = new ScheduledTaskConfig(
                "test-schedule",
                "0 2 23 ? * *",
                1,
                Criticality.MINOR,
                Recovery.SELF_HEALING
        );
        int insertSchedule = schedulerRep.createSchedule(config);
        Instant initialNextRun = schedulerRep.getSchedule("test-schedule").orElseThrow().getNextRun();

        // :: Act
        // Move clock forward to simulate the update
        LocalDateTime updateTime = LocalDateTime.of(2021, 3, 4, 12, 12);
        _clock.setFixedClock(updateTime);
        schedulerRep.updateNextRun("test-schedule");


        // :: Assert
        assertEquals(1, insertSchedule);
        Optional<Schedule> schedule = schedulerRep.getSchedule("test-schedule");
        assertTrue(schedule.isPresent());
        assertEquals(getInstant(2021, 03, 03, 23,2), initialNextRun);
        assertEquals(getInstant(2021, 03, 04, 23,2), schedule.get().getNextRun());
        // No cron expression should be set since we are not overriding the cron expression
        assertNull(schedule.get().getOverriddenCronExpression().orElse(null));
        assertEquals(updateTime.atZone(ZoneId.systemDefault()).toInstant(), schedule.get().getLastUpdated());
    }

    @Test
    public void setTaskOverridenCron() {
        // :: Setup
        InMemoryScheduledTaskRepository schedulerRep = new InMemoryScheduledTaskRepository(_clock);
        LocalDateTime insertTime = LocalDateTime.of(2021, 3, 3, 12, 1);
        _clock.setFixedClock(insertTime);
        // Create the initial schedule with a cron expression
        ScheduledTaskConfig config = new ScheduledTaskConfig(
                "test-schedule",
                "0 2 23 ? * *",
                1,
                Criticality.MINOR,
                Recovery.SELF_HEALING
        );
        int insertSchedule = schedulerRep.createSchedule(config);
        Instant initialNextRun = schedulerRep.getSchedule("test-schedule").orElseThrow().getNextRun();

        // :: Act
        // Move the clock forward 1 day to simulate the update
        LocalDateTime updateTime = LocalDateTime.of(2021, 3, 4, 12, 12);
        _clock.setFixedClock(updateTime);
        // Set a overridden cron expression
        schedulerRep.setTaskOverridenCron("test-schedule", "0 0 11 ? * *");


        // :: Assert
        assertEquals(1, insertSchedule);
        Optional<Schedule> schedule = schedulerRep.getSchedule("test-schedule");
        assertTrue(schedule.isPresent());
        assertEquals(getInstant(2021, 03, 03, 23,2), initialNextRun);
        // The next run should be the next day at 11:00 (next day after 2021-03-04)
        assertEquals(getInstant(2021, 03, 05, 11,0), schedule.get().getNextRun());
        // No cron expression should be set since we are not overriding the cron expression
        assertEquals("0 0 11 ? * *", schedule.get().getOverriddenCronExpression().orElse(null));
        assertEquals(updateTime.atZone(ZoneId.systemDefault()).toInstant(), schedule.get().getLastUpdated());
    }

    @Test
    public void setTaskOverridenCronAndClear() {
        // :: Setup
        InMemoryScheduledTaskRepository schedulerRep = new InMemoryScheduledTaskRepository(_clock);
        LocalDateTime insertTime = LocalDateTime.of(2021, 3, 3, 12, 1);
        _clock.setFixedClock(insertTime);
        // Create the initial schedule with a cron expression
        ScheduledTaskConfig config = new ScheduledTaskConfig(
                "test-schedule",
                "0 2 23 ? * *",
                1,
                Criticality.MINOR,
                Recovery.SELF_HEALING
        );
        int insertSchedule = schedulerRep.createSchedule(config);
        Instant initialNextRun = schedulerRep.getSchedule("test-schedule").orElseThrow().getNextRun();
        // Move the clock forward 1 day to simulate the update
        _clock.setFixedClock(LocalDateTime.of(2021, 3, 4, 12, 12));
        schedulerRep.setTaskOverridenCron("test-schedule", "0 0 11 ? * *");
        Schedule scheduleAfterOverride = schedulerRep.getSchedule("test-schedule").orElseThrow();
        Instant initialNextRunAfterOverride = scheduleAfterOverride.getNextRun();

        // :: Act
        // Move the clock forward 1 day to simulate the update and clear the overridden cron expression
        LocalDateTime updateTimeAfterClear = LocalDateTime.of(2021, 3, 5, 12, 12);
        _clock.setFixedClock(updateTimeAfterClear);

        // Set a overridden cron expression
        schedulerRep.setTaskOverridenCron("test-schedule", null);
        Instant initialNextRunAfterClear = schedulerRep.getSchedule("test-schedule").orElseThrow().getNextRun();


        // :: Assert
        assertEquals(1, insertSchedule);
        Optional<Schedule> schedule = schedulerRep.getSchedule("test-schedule");
        assertTrue(schedule.isPresent());
        assertEquals(getInstant(2021, 03, 03, 23,2), initialNextRun);
        // The next run after the override should be the next day at 11:00 (next day after 2021-03-04)
        assertEquals(getInstant(2021, 03, 05, 11,0), initialNextRunAfterOverride);
        // The next run after we clear the override should be later at 23:02 (2021-03-05)
        assertEquals(getInstant(2021, 03, 05, 23,2), initialNextRunAfterClear);
        // A overrideExpression should be set before we clear it.
        assertEquals("0 0 11 ? * *", scheduleAfterOverride.getOverriddenCronExpression().orElse(null));
        // No cron expression should be set since we have cleared the overridden cron expression
        assertNull(schedule.get().getOverriddenCronExpression().orElse(null));
        assertEquals(updateTimeAfterClear.atZone(ZoneId.systemDefault()).toInstant(), schedule.get().getLastUpdated());
    }

    private Instant getInstant(int year, int month, int dayOfMonth, int hour, int minute) {
        LocalDateTime nextRunTime = LocalDateTime.of(year, month, dayOfMonth, hour, minute);
        return nextRunTime.atZone(ZoneId.systemDefault()).toInstant();
    }

    @Test
    public void setActive() {
        // :: Setup
        InMemoryScheduledTaskRepository schedulerRep = new InMemoryScheduledTaskRepository(_clock);
        LocalDateTime insertTime = LocalDateTime.of(2021, 3, 3, 12, 1);
        _clock.setFixedClock(insertTime);
        ScheduledTaskConfig config = new ScheduledTaskConfig(
                "test-schedule",
                "0 2 23 ? * *",
                1,
                Criticality.MINOR,
                Recovery.SELF_HEALING
        );
        schedulerRep.createSchedule(config);

        // :: Act
        LocalDateTime updateTime = LocalDateTime.of(2021, 3, 3, 12, 12);
        _clock.setFixedClock(updateTime);
        Optional<Schedule> beforeSettingInactive = schedulerRep.getSchedule("test-schedule");
        schedulerRep.setActive("test-schedule", false);

        // :: Assert
        assertTrue(beforeSettingInactive.isPresent());
        Optional<Schedule> afterSetInactive = schedulerRep.getSchedule("test-schedule");
        assertTrue(afterSetInactive.isPresent());
        assertTrue(beforeSettingInactive.get().isActive());
        assertFalse(afterSetInactive.get().isActive());
        // Since no overriden cron is set we should not have set any new cron expression
        assertNull(beforeSettingInactive.get().getOverriddenCronExpression().orElse(null));
        Instant insertTimeInstant = insertTime.atZone(ZoneId.systemDefault()).toInstant();
        assertEquals(insertTimeInstant, beforeSettingInactive.get().getLastUpdated());
        // Since no overriden cron is set we should not have set any new cron expression
        assertNull(afterSetInactive.get().getOverriddenCronExpression().orElse(null));
        assertEquals(insertTimeInstant, afterSetInactive.get().getLastUpdated());
    }

    @Test
    public void setRunOnce() {
        // :: Setup
        InMemoryScheduledTaskRepository schedulerRep = new InMemoryScheduledTaskRepository(_clock);
        LocalDateTime insertTime = LocalDateTime.of(2021, 3, 3, 12, 1);
        _clock.setFixedClock(insertTime);
        ScheduledTaskConfig config = new ScheduledTaskConfig(
                "test-schedule",
                "0 2 23 ? * *",
                1,
                Criticality.MINOR,
                Recovery.SELF_HEALING
        );
        schedulerRep.createSchedule(config);

        // :: Act
        LocalDateTime updateTime = LocalDateTime.of(2021, 3, 3, 12, 12);
        _clock.setFixedClock(updateTime);
        Optional<Schedule> beforeSettingRunOnce = schedulerRep.getSchedule("test-schedule");
        schedulerRep.setRunOnce("test-schedule", RunOnce.PROGRAMMATIC);

        // :: Assert
        assertTrue(beforeSettingRunOnce.isPresent());
        Optional<Schedule> afterSetRunOnce = schedulerRep.getSchedule("test-schedule");
        assertTrue(afterSetRunOnce.isPresent());
        assertFalse(beforeSettingRunOnce.get().isRunOnce());
        assertTrue(afterSetRunOnce.get().isRunOnce());
        // No cron expression should be set since we are not overriding the cron expression
        assertNull(beforeSettingRunOnce.get().getOverriddenCronExpression().orElse(null));
        Instant insertTimeInstant = insertTime.atZone(ZoneId.systemDefault()).toInstant();
        assertEquals(insertTimeInstant, beforeSettingRunOnce.get().getLastUpdated());
        // No cron expression should be set since we are not overriding the cron expression
        assertNull(afterSetRunOnce.get().getOverriddenCronExpression().orElse(null));
        assertEquals(insertTimeInstant, afterSetRunOnce.get().getLastUpdated());
        assertFalse(beforeSettingRunOnce.get().getRunOnce().isPresent());
        assertEquals(RunOnce.PROGRAMMATIC, afterSetRunOnce.get().getRunOnce().orElse(null));
    }

    @Test
    public void getSchedules() {
        // :: Setup
        InMemoryScheduledTaskRepository schedulerRep = new InMemoryScheduledTaskRepository(_clock);
        LocalDateTime now = LocalDateTime.of(2021, 3, 3, 12, 1);
        _clock.setFixedClock(now);
        ScheduledTaskConfig configSchedule1 = new ScheduledTaskConfig(
                "test-schedule-1",
                "0 0 23 ? * *",
                1,
                Criticality.MINOR,
                Recovery.SELF_HEALING
        );
        ScheduledTaskConfig configSchedule2 = new ScheduledTaskConfig(
                "test-schedule-2",
                "0 0 23 ? * *",
                1,
                Criticality.MINOR,
                Recovery.SELF_HEALING
        );
        schedulerRep.createSchedule(configSchedule1);
        schedulerRep.createSchedule(configSchedule2);

        // :: Act
        Map<String, Schedule> schedules = schedulerRep.getSchedules();

        // :: Assert
        assertEquals(2, schedules.size());
        Schedule schedule1 = schedules.get("test-schedule-1");
        assertNotNull(schedule1);
        assertEquals(now.atZone(ZoneId.systemDefault()).toInstant(), schedule1.getLastUpdated());
    }

    // ==== Scheduled runs tests =================================================================

    @Test
    public void addScheduleRun_ok() {
        // :: Setup
        InMemoryScheduledTaskRepository schedulerRep = new InMemoryScheduledTaskRepository(_clock);
        LocalDateTime now = LocalDateTime.of(2021, 3, 3, 12, 1);
        Instant nowInstant = now.atZone(ZoneId.systemDefault()).toInstant();
        _clock.setFixedClock(now);

        // :: Act
        long id = schedulerRep.addScheduleRun("test-schedule", "some-hostname",
                nowInstant, "schedule run inserted");

        // :: Assert
        assertTrue(id > 0);
        Optional<ScheduledRunDto> scheduleRun = schedulerRep.getScheduledRun(id);
        assertTrue(scheduleRun.isPresent());
        assertEquals("schedule run inserted", scheduleRun.get().getStatusMsg());
        assertNull(scheduleRun.get().getStatusStackTrace());
        assertEquals(now, scheduleRun.get().getRunStart());
        assertEquals(0, schedulerRep.getLogEntries(id).size());
        assertEquals(now, scheduleRun.get().getStatusTime());
        Assertions.assertEquals(ScheduledTaskRegistry.State.STARTED, scheduleRun.get().getStatus());
    }

    @Test
    public void setStatusDone_ok() {
        // :: Setup
        InMemoryScheduledTaskRepository schedulerRep = new InMemoryScheduledTaskRepository(_clock);
        LocalDateTime now = LocalDateTime.of(2021, 3, 3, 12, 1);
        Instant nowInstant = now.atZone(ZoneId.systemDefault()).toInstant();
        _clock.setFixedClock(now);
        long inserted = schedulerRep.addScheduleRun("test-schedule", "some-hostname",
                nowInstant, "schedule run inserted");

        // :: Act
        schedulerRep.setStatus(inserted, ScheduledTaskRegistry.State.DONE, "Updating state to DONE",
                null, Instant.now(_clock));

        // :: Assert
        assertTrue(inserted > 0);
        Optional<ScheduledRunDto> scheduleRun = schedulerRep.getScheduledRun(inserted);
        assertTrue(scheduleRun.isPresent());
        assertEquals("Updating state to DONE", scheduleRun.get().getStatusMsg());
        assertEquals(ScheduledTaskRegistry.State.DONE, scheduleRun.get().getStatus());
        assertNull(scheduleRun.get().getStatusStackTrace());
        assertEquals(now, scheduleRun.get().getRunStart());
        assertEquals(0, schedulerRep.getLogEntries(inserted).size());
        assertEquals(now, scheduleRun.get().getStatusTime());
    }

    @Test
    public void setStatusFail_ok() {
        // :: Setup
        InMemoryScheduledTaskRepository schedulerRep = new InMemoryScheduledTaskRepository(_clock);
        LocalDateTime now = LocalDateTime.of(2021, 3, 3, 12, 1);
        Instant nowInstant = now.atZone(ZoneId.systemDefault()).toInstant();
        _clock.setFixedClock(now);
        long inserted = schedulerRep.addScheduleRun("test-schedule", "some-hostname",
                nowInstant, "schedule run inserted");

        // :: Act
        schedulerRep.setStatus(inserted, ScheduledTaskRegistry.State.FAILED, "This run failed",
                null, Instant.now(_clock));

        // :: Assert
        assertTrue(inserted > 0);
        Optional<ScheduledRunDto> scheduleRun = schedulerRep.getScheduledRun(inserted);
        assertTrue(scheduleRun.isPresent());
        assertEquals("This run failed", scheduleRun.get().getStatusMsg());
        assertEquals(ScheduledTaskRegistry.State.FAILED, scheduleRun.get().getStatus());
        assertNull(scheduleRun.get().getStatusStackTrace());
        assertEquals(now, scheduleRun.get().getRunStart());
        assertEquals(0, schedulerRep.getLogEntries(inserted).size());
        assertEquals(now, scheduleRun.get().getStatusTime());
    }

    @Test
    public void setStatusFailWithThrowable_ok() {
        // :: Setup
        InMemoryScheduledTaskRepository schedulerRep = new InMemoryScheduledTaskRepository(_clock);
        LocalDateTime now = LocalDateTime.of(2021, 3, 3, 12, 1);
        Instant nowInstant = now.atZone(ZoneId.systemDefault()).toInstant();
        _clock.setFixedClock(now);
        long inserted = schedulerRep.addScheduleRun("test-schedule", "some-hostname",
                nowInstant, "schedule run inserted");

        // :: Act
        schedulerRep.setStatus(inserted, ScheduledTaskRegistry.State.FAILED, "This run failed",
                "some exception", Instant.now(_clock));

        // :: Assert
        assertTrue(inserted > 0);
        Optional<ScheduledRunDto> scheduleRun = schedulerRep.getScheduledRun(inserted);
        assertTrue(scheduleRun.isPresent());
        assertEquals("This run failed", scheduleRun.get().getStatusMsg());
        assertEquals(ScheduledTaskRegistry.State.FAILED, scheduleRun.get().getStatus());
        assertNotNull(scheduleRun.get().getStatusStackTrace());
        assertEquals(now, scheduleRun.get().getRunStart());
        assertEquals(0, schedulerRep.getLogEntries(inserted).size());
        assertEquals(now, scheduleRun.get().getStatusTime());
    }

    @Test
    public void setFailedSecondTime_fail() {
        // :: Setup
        InMemoryScheduledTaskRepository schedulerRep = new InMemoryScheduledTaskRepository(_clock);
        LocalDateTime now = LocalDateTime.of(2021, 3, 3, 12, 1);
        Instant nowInstant = now.atZone(ZoneId.systemDefault()).toInstant();
        _clock.setFixedClock(now);
        long inserted = schedulerRep.addScheduleRun("test-schedule", "some-hostname",
                nowInstant, "schedule run inserted");

        // :: Act
        boolean setFailed1 = schedulerRep.setStatus(inserted, ScheduledTaskRegistry.State.FAILED,
                "Fault added after insert", "testing failed exception", Instant.now(_clock));
        boolean setFailed2 = schedulerRep.setStatus(inserted, ScheduledTaskRegistry.State.FAILED,
                "Fault added a second time", "testing 2 exception", Instant.now(_clock));

        // :: Assert
        assertTrue(inserted > 0);
        assertTrue(setFailed1);
        // Should be no limit on changing end state, IE we are allowed to set the same state multiple times.
        assertTrue(setFailed2);
        Optional<ScheduledRunDto> scheduleRun = schedulerRep.getScheduledRun(inserted);
        assertTrue(scheduleRun.isPresent());
        assertEquals("Fault added a second time", scheduleRun.get().getStatusMsg());
        assertEquals("testing 2 exception", scheduleRun.get().getStatusStackTrace());
        assertEquals(now, scheduleRun.get().getRunStart());
        assertEquals(0, schedulerRep.getLogEntries(inserted).size());
        assertEquals(now, scheduleRun.get().getStatusTime());
        assertEquals(ScheduledTaskRegistry.State.FAILED, scheduleRun.get().getStatus());
    }

    @Test
    public void setFailedToDone_Ok() {
        // :: Setup
        InMemoryScheduledTaskRepository schedulerRep = new InMemoryScheduledTaskRepository(_clock);
        LocalDateTime now = LocalDateTime.of(2021, 3, 3, 12, 1);
        Instant nowInstant = now.atZone(ZoneId.systemDefault()).toInstant();
        _clock.setFixedClock(now);
        long inserted = schedulerRep.addScheduleRun("test-schedule", "some-hostname",
                nowInstant, "schedule run inserted");
        boolean setFailed = schedulerRep.setStatus(inserted, ScheduledTaskRegistry.State.FAILED,
                "fault added after insert", "testing failed exception", Instant.now(_clock));

        // :: Act
        boolean setDone = schedulerRep.setStatus(inserted, ScheduledTaskRegistry.State.DONE,
                "Updated status from failed to done", "second testing failed exception",
                Instant.now(_clock));

        // :: Assert
        assertTrue(inserted > 0);
        assertTrue(setFailed);
        // Should be no limit on changing end state
        assertTrue(setDone);
        Optional<ScheduledRunDto> scheduleRun = schedulerRep.getScheduledRun(inserted);
        assertTrue(scheduleRun.isPresent());
        assertEquals("Updated status from failed to done", scheduleRun.get().getStatusMsg());
        assertEquals("second testing failed exception", scheduleRun.get().getStatusStackTrace());
        assertEquals(now, scheduleRun.get().getRunStart());
        assertEquals(0, schedulerRep.getLogEntries(inserted).size());
        assertEquals(now, scheduleRun.get().getStatusTime());
        assertEquals(ScheduledTaskRegistry.State.DONE, scheduleRun.get().getStatus());
    }

    @Test
    public void setDoneToFail_ok() {
        // :: Setup
        InMemoryScheduledTaskRepository schedulerRep = new InMemoryScheduledTaskRepository(_clock);
        LocalDateTime now = LocalDateTime.of(2021, 3, 3, 12, 1);
        Instant nowInstant = now.atZone(ZoneId.systemDefault()).toInstant();
        _clock.setFixedClock(now);
        long inserted = schedulerRep.addScheduleRun("test-schedule", "some-hostname",
                nowInstant, "schedule run inserted");
        boolean setDone = schedulerRep.setStatus(inserted, ScheduledTaskRegistry.State.DONE,
                "done set after insert", null, Instant.now(_clock));

        // :: Act
        boolean setFailed = schedulerRep.setStatus(inserted, ScheduledTaskRegistry.State.FAILED,
                "Updated status from done to failed", "testing failed exception", Instant.now(_clock));


        // :: Assert
        assertTrue(inserted > 0);
        assertTrue(setDone);
        // Should be no limit on changing end state
        assertTrue(setFailed);
        Optional<ScheduledRunDto> scheduleRun = schedulerRep.getScheduledRun(inserted);
        assertTrue(scheduleRun.isPresent());
        assertEquals("Updated status from done to failed", scheduleRun.get().getStatusMsg());
        assertEquals("testing failed exception", scheduleRun.get().getStatusStackTrace());
        assertEquals(now, scheduleRun.get().getRunStart());
        assertEquals(0, schedulerRep.getLogEntries(inserted).size());
        assertEquals(now, scheduleRun.get().getStatusTime());
        assertEquals(ScheduledTaskRegistry.State.FAILED, scheduleRun.get().getStatus());
    }

    @Test
    public void setDispatchedToFail_ok() {
        // :: Setup
        InMemoryScheduledTaskRepository schedulerRep = new InMemoryScheduledTaskRepository(_clock);
        LocalDateTime now = LocalDateTime.of(2021, 3, 3, 12, 1);
        Instant nowInstant = now.atZone(ZoneId.systemDefault()).toInstant();
        _clock.setFixedClock(now);
        long inserted = schedulerRep.addScheduleRun("test-schedule", "some-hostname",
                nowInstant, "schedule run inserted");
        boolean setDispatched = schedulerRep.setStatus(inserted, ScheduledTaskRegistry.State.DISPATCHED,
                "Dispatch set after insert", null, Instant.now(_clock));

        // :: Act
        LocalDateTime failTime = LocalDateTime.of(2021, 3, 3, 12, 2);
        _clock.setFixedClock(failTime);
        boolean setFailed = schedulerRep.setStatus(inserted, ScheduledTaskRegistry.State.FAILED,
                "Dispatched to fail is ok", "testing failed exception", Instant.now(_clock));


        // :: Assert
        assertTrue(inserted > 0);
        assertTrue(setDispatched);
        assertTrue(setFailed);
        Optional<ScheduledRunDto> scheduleRun = schedulerRep.getScheduledRun(inserted);
        assertTrue(scheduleRun.isPresent());
        assertEquals("Dispatched to fail is ok", scheduleRun.get().getStatusMsg());
        assertEquals("testing failed exception",
                scheduleRun.get().getStatusStackTrace());
        assertEquals(now, scheduleRun.get().getRunStart());
        assertEquals(0, schedulerRep.getLogEntries(inserted).size());
        assertEquals(failTime, scheduleRun.get().getStatusTime());
        assertEquals(ScheduledTaskRegistry.State.FAILED, scheduleRun.get().getStatus());
    }

    @Test
    public void setDispatchedToDispatchedToDone_ok() {
        // :: Setup
        InMemoryScheduledTaskRepository schedulerRep = new InMemoryScheduledTaskRepository(_clock);
        LocalDateTime now = LocalDateTime.of(2021, 3, 3, 12, 1);
        Instant nowInstant = now.atZone(ZoneId.systemDefault()).toInstant();
        _clock.setFixedClock(now);
        long inserted = schedulerRep.addScheduleRun("test-schedule", "some-hostname",
                nowInstant, "schedule run inserted");
        boolean firstDispatched = schedulerRep.setStatus(inserted, ScheduledTaskRegistry.State.DISPATCHED,
                "Dispatch set after insert", null, Instant.now(_clock));

        // :: Act
        _clock.setFixedClock(LocalDateTime.of(2021, 3, 3, 12, 2));
        boolean secondDispatched = schedulerRep.setStatus(inserted, ScheduledTaskRegistry.State.DISPATCHED,
                "Second Dispatch", null, Instant.now(_clock));
        LocalDateTime doneTime = LocalDateTime.of(2021, 3, 3, 12, 2);
        _clock.setFixedClock(doneTime);
        boolean setDone = schedulerRep.setStatus(inserted, ScheduledTaskRegistry.State.DONE,
                "Dispatched to done is ok", null, Instant.now(_clock));

        // :: Assert
        assertTrue(inserted > 0);
        assertTrue(firstDispatched);
        assertTrue(secondDispatched);
        assertTrue(setDone);
        Optional<ScheduledRunDto> scheduleRun = schedulerRep.getScheduledRun(inserted);
        assertTrue(scheduleRun.isPresent());
        assertEquals("Dispatched to done is ok", scheduleRun.get().getStatusMsg());
        assertNull(scheduleRun.get().getStatusStackTrace());
        assertEquals(now, scheduleRun.get().getRunStart());
        assertEquals(0, schedulerRep.getLogEntries(inserted).size());
        assertEquals(doneTime, scheduleRun.get().getStatusTime());
        assertEquals(ScheduledTaskRegistry.State.DONE, scheduleRun.get().getStatus());
    }

    @Test
    public void addLogEntry_ok() {
        // :: Setup
        InMemoryScheduledTaskRepository schedulerRep = new InMemoryScheduledTaskRepository(_clock);
        LocalDateTime now = LocalDateTime.of(2021, 3, 3, 12, 1);
        Instant nowInstant = now.atZone(ZoneId.systemDefault()).toInstant();
        _clock.setFixedClock(now);
        long runId = schedulerRep.addScheduleRun("test-schedule", "some-hostname",
                nowInstant, "schedule run inserted");

        // :: Act
        LocalDateTime logTime = LocalDateTime.of(2021, 3, 3, 12, 2);
        schedulerRep.addLogEntry(runId, logTime, "some log message");

        // :: Assert
        List<LogEntry> logEntryFromDb = schedulerRep.getLogEntries(runId);
        assertEquals(1, logEntryFromDb.size());
        assertEquals("some log message", logEntryFromDb.get(0).getMessage());
        assertFalse(logEntryFromDb.get(0).getStackTrace().isPresent());
        assertEquals(logTime, logEntryFromDb.get(0).getLogTime());
    }

    @Test
    public void addLogEntryWithThrowable_ok() {
        // :: Setup
        InMemoryScheduledTaskRepository schedulerRep = new InMemoryScheduledTaskRepository(_clock);
        LocalDateTime now = LocalDateTime.of(2021, 3, 3, 12, 1);
        Instant nowInstant = now.atZone(ZoneId.systemDefault()).toInstant();
        _clock.setFixedClock(now);
        long runId = schedulerRep.addScheduleRun("test-schedule", "some-instance-id",
                nowInstant, "schedule run inserted");

        // :: Act
        schedulerRep.addLogEntry(runId, now, "some log message", "testing throwable");

        // :: Assert
        List<LogEntry> logEntryFromDb = schedulerRep.getLogEntries(runId);
        assertEquals(1, logEntryFromDb.size());
        LogEntry firstLogEntry = logEntryFromDb.get(0);
        assertEquals("some log message", firstLogEntry.getMessage());
        assertEquals("testing throwable", firstLogEntry.getStackTrace().get());
        assertEquals(now, firstLogEntry.getLogTime());
    }

    @Test
    public void getLastRunForSchedule_ok() {
        // :: Setup
        InMemoryScheduledTaskRepository schedulerRep = new InMemoryScheduledTaskRepository(_clock);
        _clock.setFixedClock(LocalDateTime.of(2021, 3, 3, 12, 1));

        // first - test-schedule-1
        long runId1 = schedulerRep.addScheduleRun("test-schedule-1", "instance-id-firs-run",
                LocalDateTime.of(2021, 3, 3, 12, 1).atZone(ZoneId.systemDefault()).toInstant(),
                "first run schedule 1");
        schedulerRep.addLogEntry(runId1, LocalDateTime.of(2021, 3, 3, 12, 2), "some log message");

        // second - test-schedule-2
        _clock.setFixedClock(LocalDateTime.of(2021, 3, 3, 12, 10));
        long runId2 = schedulerRep.addScheduleRun("test-schedule-2", "instance-id-second-run",
                LocalDateTime.of(2021, 3, 3, 12, 10).atZone(ZoneId.systemDefault()).toInstant(),
                "first run schedule 2");
        schedulerRep.addLogEntry(runId2, LocalDateTime.of(2021, 3, 3, 12, 12),
                "second schedule log entry");

        // third - test-schedule-1 - this is one we want to get
        LocalDateTime lastInsertTime = LocalDateTime.of(2021, 3, 3, 12, 15);
        _clock.setFixedClock(lastInsertTime);
        long runId3 = schedulerRep.addScheduleRun("test-schedule-1", "instance-id-thirds-run",
                LocalDateTime.of(2021, 3, 3, 12, 15).atZone(ZoneId.systemDefault()).toInstant(),
                "second run schedule 1");
        LocalDateTime lastLogTime = LocalDateTime.of(2021, 3, 3, 12, 16);
        schedulerRep.addLogEntry(runId3, lastLogTime, "first schedule log entry 2");

        // forth - test-schedule-2
        _clock.setFixedClock(LocalDateTime.of(2021, 3, 3, 12, 20));
        long runId4 = schedulerRep.addScheduleRun("test-schedule-2", "instance-id-forth-run",
                LocalDateTime.of(2021, 3, 3, 12, 20).atZone(ZoneId.systemDefault()).toInstant(),
                "second run schedule 2");
        schedulerRep.addLogEntry(runId4, LocalDateTime.of(2021, 3, 3, 12, 21),
                "second schedule log entry 2");

        // :: Act
        Optional<ScheduledRunDto> scheduleRunFromDb = schedulerRep.getLastRunForSchedule("test-schedule-1");

        // :: Assert
        assertTrue(scheduleRunFromDb.isPresent());
        assertEquals(lastInsertTime, scheduleRunFromDb.get().getRunStart());
        assertEquals("instance-id-thirds-run", scheduleRunFromDb.get().getHostname());
    }

    @Test
    public void getScheduledRunWithLogsBetween_ok() {
        // :: Setup
        InMemoryScheduledTaskRepository schedulerRep = new InMemoryScheduledTaskRepository(_clock);
        _clock.setFixedClock(LocalDateTime.of(2021, 3, 3, 12, 1));

        // first - test-schedule-1 - this should not be included by the get
        long runId1 = schedulerRep.addScheduleRun("test-schedule-1", "instance-id-firs-run",
                LocalDateTime.of(2021, 3, 3, 12, 1).atZone(ZoneId.systemDefault()).toInstant(),
                "first run schedule 1");
        schedulerRep.addLogEntry(runId1, LocalDateTime.of(2021, 3, 3, 12, 2), "some log message");

        // second - test-schedule-1
        _clock.setFixedClock(LocalDateTime.of(2021, 3, 3, 12, 10));
        long runId2 = schedulerRep.addScheduleRun("test-schedule-1", "instance-id-second-run",
                LocalDateTime.of(2021, 3, 3, 12, 10).atZone(ZoneId.systemDefault()).toInstant(),
                "second run schedule 1");
        schedulerRep.addLogEntry(runId2, LocalDateTime.of(2021, 3, 3, 12, 12),
                "first schedule log entry 2");

        // third - test-schedule-1 - get from including
        _clock.setFixedClock(LocalDateTime.of(2021, 3, 3, 12, 15));
        long runId3 = schedulerRep.addScheduleRun("test-schedule-1", "instance-id-third-run",
                LocalDateTime.of(2021, 3, 3, 12, 15).atZone(ZoneId.systemDefault()).toInstant(),
                "third run schedule 1");
        _clock.setFixedClock(LocalDateTime.of(2021, 3, 3, 12, 16));
        schedulerRep.addLogEntry(runId3, LocalDateTime.of(2021, 3, 3, 12, 16),
                "first schedule log entry 3");

        // forth - test-schedule-2 - this should not be picked up due to it is another scheduleName
        _clock.setFixedClock(LocalDateTime.of(2021, 3, 3, 12, 20));
        long runId4 = schedulerRep.addScheduleRun("test-schedule-2", "instance-id-forth-run",
                LocalDateTime.of(2021, 3, 3, 12, 20).atZone(ZoneId.systemDefault()).toInstant(),
                "first run schedule 2");
        schedulerRep.addLogEntry(runId4, LocalDateTime.of(2021, 3, 3, 12, 20),
                "second schedule log entry 1");

        // fifth - test-schedule-1 - get to including
        _clock.setFixedClock(LocalDateTime.of(2021, 3, 3, 12, 25));
        long runId5 = schedulerRep.addScheduleRun("test-schedule-1", "instance-id-fifth-run",
                LocalDateTime.of(2021, 3, 3, 12, 25).atZone(ZoneId.systemDefault()).toInstant(),
                "forth run schedule 1");
        schedulerRep.addLogEntry(runId5, LocalDateTime.of(2021, 3, 3, 12, 25),
                "first schedule log entry 4");

        // sixth - test-schedule-1 - should not be included
        _clock.setFixedClock(LocalDateTime.of(2021, 3, 3, 12, 30));
        long runId6 = schedulerRep.addScheduleRun("test-schedule-1", "instance-id-sixth-run",
                LocalDateTime.of(2021, 3, 3, 12, 30).atZone(ZoneId.systemDefault()).toInstant(),
                "fifth run schedule 1");
        schedulerRep.addLogEntry(runId6, LocalDateTime.of(2021, 3, 3, 12, 30),
                "first schedule log entry 5");

        // :: Act
        List<ScheduledRunDto> scheduleRunFromDb = schedulerRep.getScheduleRunsBetween(
                "test-schedule-1",
                LocalDateTime.of(2021, 3, 3, 12, 12),
                LocalDateTime.of(2021, 3, 3, 12, 26));

        // :: Assert
        assertEquals(2, scheduleRunFromDb.size());
        List<LogEntry> logEntriesFromDb1 = schedulerRep.getLogEntries(scheduleRunFromDb.get(0).getRunId());
        assertEquals(1, logEntriesFromDb1.size());
        // Get the second schedule's logEntries
        List<LogEntry> logEntriesFromDb2 = schedulerRep.getLogEntries(scheduleRunFromDb.get(1).getRunId());
        assertEquals("first schedule log entry 3", logEntriesFromDb2.get(0).getMessage());
        assertEquals("first schedule log entry 4", logEntriesFromDb1.get(0).getMessage());
        assertFalse(logEntriesFromDb1.get(0).getStackTrace().isPresent());
        // Show that the runTime can differ from the logTime
        assertEquals(LocalDateTime.of(2021, 3, 3, 12, 15),
                scheduleRunFromDb.get(1).getRunStart());
        assertEquals(LocalDateTime.of(2021, 3, 3, 12, 16),
                logEntriesFromDb2.get(0).getLogTime());
        assertFalse(logEntriesFromDb2.get(0).getStackTrace().isPresent());

    }


    @Test
    public void getLastScheduleRuns_ok() {
        // :: Setup
        InMemoryScheduledTaskRepository schedulerRep = new InMemoryScheduledTaskRepository(_clock);
        _clock.setFixedClock(LocalDateTime.of(2021, 3, 3, 12, 1));

        // first - test-schedule-1 - this should not be included by the get
        long runId1 = schedulerRep.addScheduleRun("test-schedule-1", "instance-id-firs-run",
                LocalDateTime.of(2021, 3, 3, 12, 1).atZone(ZoneId.systemDefault()).toInstant(),
                "first run schedule 1");
        schedulerRep.addLogEntry(runId1, LocalDateTime.of(2021, 3, 3, 12, 2), "some log message");

        // second - test-schedule-1
        long runId2 = schedulerRep.addScheduleRun("test-schedule-1", "instance-id-second-run",
                LocalDateTime.of(2021, 3, 3, 12, 10).atZone(ZoneId.systemDefault()).toInstant(),
                "second run schedule 1");
        schedulerRep.addLogEntry(runId2, LocalDateTime.of(2021, 3, 3, 12, 10),
                "first schedule log entry 2");

        // third - test-schedule-1
        long runId3 = schedulerRep.addScheduleRun("test-schedule-1", "instance-id-third-run",
                LocalDateTime.of(2021, 3, 3, 12, 15).atZone(ZoneId.systemDefault()).toInstant(),
                "third run schedule 1");
        schedulerRep.addLogEntry(runId3, LocalDateTime.of(2021, 3, 3, 12, 16),
                "first schedule log entry 3");

        // forth - test-schedule-2 - first from schedule 2
        long runId4 = schedulerRep.addScheduleRun("test-schedule-2", "instance-id-forth-run",
                LocalDateTime.of(2021, 3, 3, 12, 20).atZone(ZoneId.systemDefault()).toInstant(),
                "first run schedule 2");
        schedulerRep.addLogEntry(runId4, LocalDateTime.of(2021, 3, 3, 12, 20),
                "second schedule log entry 1");

        // fifth - test-schedule-1
        _clock.setFixedClock(LocalDateTime.of(2021, 3, 3, 12, 25));
        long runId5 = schedulerRep.addScheduleRun("test-schedule-1", "instance-id-fifth-run",
                LocalDateTime.of(2021, 3, 3, 12, 25).atZone(ZoneId.systemDefault()).toInstant(),
                "forth run schedule 1");
        schedulerRep.addLogEntry(runId5, LocalDateTime.of(2021, 3, 3, 12, 25),
                "first schedule log entry 4");

        // sixth - test-schedule-1 - should should be last run done and the one we want to retrieve
        LocalDateTime lastRunTimeSchedule1 = LocalDateTime.of(2021, 3, 3, 12, 30);
        long runId6 = schedulerRep.addScheduleRun("test-schedule-1", "instance-id-sixth-run",
                lastRunTimeSchedule1.atZone(ZoneId.systemDefault()).toInstant(),
                "fifth run schedule 1");
        schedulerRep.addLogEntry(runId6, LocalDateTime.of(2021, 3, 3, 12, 31),
                "first schedule log entry 5");

        // seventh - schedule-2 - this is the last run for the schedule 2 that we want to retrieve
        LocalDateTime lastRunTimeSchedule2 = LocalDateTime.of(2021, 3, 3, 12, 32);
        long runId7 = schedulerRep.addScheduleRun("test-schedule-2", "instance-id-seventh-run",
                lastRunTimeSchedule2.atZone(ZoneId.systemDefault()).toInstant(),
                "second run schedule 2");
        schedulerRep.addLogEntry(runId7, LocalDateTime.of(2021, 3, 3, 12, 33),
                "second schedule log entry 2");

        // :: Act
        List<ScheduledRunDto> scheduleRunFromDb = schedulerRep.getLastScheduleRuns();

        // :: Assert
        assertEquals(2, scheduleRunFromDb.size());
        // We should have the last run first, IE the second schedule entry 2.

        // Show that the runTime can differ from the logTime
        assertEquals(lastRunTimeSchedule2, scheduleRunFromDb.get(0).getRunStart());
    }


}