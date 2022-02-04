package com.storebrand.scheduledtask;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Optional;

import javax.sql.DataSource;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;

import com.storebrand.scheduledtask.MasterLockRepositoryTest.ClockMock;
import com.storebrand.scheduledtask.ScheduledTaskRepository.ScheduledRunDbo;
import com.storebrand.scheduledtask.ScheduledTaskService.LogEntry;
import com.storebrand.scheduledtask.ScheduledTaskServiceImpl.LogEntryImpl;
import com.storebrand.scheduledtask.ScheduledTaskServiceImpl.ScheduleDto;
import com.storebrand.scheduledtask.ScheduledTaskServiceImpl.ScheduledRunDto;
import com.storebrand.scheduledtask.ScheduledTaskServiceImpl.State;
import com.storebrand.scheduledtask.internal.cron.CronExpression;

/**
 * Tests for {@link ScheduledTaskRepository}
 *
 * @author Dag Bertelsen - dag.lennart.bertelsen@storebrand.no - dabe@dagbertelsen.com - 2021.03
 */
public class ScheduledTaskRepositoryTest {
    private final JdbcTemplate _jdbcTemplate;
    private final DataSource _dataSource;
    private final ClockMock _clock = new ClockMock();
    static final String STOREBRAND_SCHEDULE_CREATE_SQL = "CREATE TABLE " + ScheduledTaskRepository.SCHEDULE_TASK_TABLE + " ( "
            + " schedule_name VARCHAR NOT NULL, "
            + " is_active BIT NOT NULL, "
            + " run_once BIT NOT NULL, "
            + " cron_expression VARCHAR NULL, "
            + " next_run DATETIME2 NOT NULL, "
            + " last_updated DATETIME2 NOT NULL, "
            + " CONSTRAINT PK_schedule_name PRIMARY KEY (schedule_name) "
            + " );";

    static final String SCHEDULE_RUN_CREATE_SQL = "CREATE TABLE " + ScheduledTaskRepository.SCHEDULE_RUN_TABLE + " ( "
            + " schedule_name VARCHAR NOT NULL, "
            + " instance_id VARCHAR NOT NULL, "
            + " run_start DATETIME2 NOT NULL, "
            + " status VARCHAR NULL, "
            + " status_msg VARCHAR NULL, "
            + " status_throwable VARCHAR NULL, "
            + " status_time DATETIME2 NOT NULL, "
            + " CONSTRAINT PK_instance_id PRIMARY KEY (instance_id) "
            + " );";

    static final String SCHEDULE_LOG_ENTRY_CREATE_SQL = "CREATE TABLE " + ScheduledTaskRepository.SCHEDULE_LOG_ENTRY_TABLE + " ( "
            + " instance_id VARCHAR NOT NULL, "
            + " log_msg VARCHAR NOT NULL, "
            + " log_throwable VARCHAR NULL, "
            + " log_time DATETIME2 NOT NULL, "
            + " CONSTRAINT FK_instance_id FOREIGN KEY (instance_id) REFERENCES stb_schedule_run (instance_id) "
            + " );";

    public ScheduledTaskRepositoryTest() {
        _dataSource = new SingleConnectionDataSource("jdbc:h2:mem:testStorebrandSchedulerDb", true);
        _jdbcTemplate = new JdbcTemplate(_dataSource);
    }

    @BeforeEach
    public void before() {
        _jdbcTemplate.execute(STOREBRAND_SCHEDULE_CREATE_SQL);
        _jdbcTemplate.execute(SCHEDULE_RUN_CREATE_SQL);
        _jdbcTemplate.execute(SCHEDULE_LOG_ENTRY_CREATE_SQL);
    }

    @AfterEach
    public void after() {
        _jdbcTemplate.execute("DROP TABLE " + ScheduledTaskRepository.SCHEDULE_LOG_ENTRY_TABLE + ";");
        _jdbcTemplate.execute("DROP TABLE " + ScheduledTaskRepository.SCHEDULE_RUN_TABLE + ";");
        _jdbcTemplate.execute("DROP TABLE " + ScheduledTaskRepository.SCHEDULE_TASK_TABLE + ";");
    }

    @Test
    public void createScheduleThatDoesNotExists_ok() {
        // :: Setup
        ScheduledTaskRepository schedulerRep = new ScheduledTaskRepository(_dataSource, _clock);
        Instant nextRun = getInstant(2021, 3, 3, 12, 24);

        // :: Act
        int created = schedulerRep.createSchedule("testSchedule", CronExpression.parse("0 0 23 ? * *"), nextRun);

        // :: Assert
        assertEquals(1, created);
        Optional<ScheduleDto> schedule = schedulerRep.getSchedule("testSchedule");
        assertTrue(schedule.isPresent());
        assertEquals(nextRun, schedule.get().getNextRun());
        assertTrue(schedule.get().isActive());
        assertFalse(schedule.get().isRunOnce());
    }

    @Test
    public void createScheduleThatAlreadyExists_fail() {
        // :: Setup
        ScheduledTaskRepository schedulerRep = new ScheduledTaskRepository(_dataSource, _clock);
        Instant nextRun = getInstant(2021, 3, 3, 12, 24);
        int firstInsert = schedulerRep.createSchedule("alreadyExists", null, nextRun);

        // :: Act
        int secondInsert = schedulerRep.createSchedule("alreadyExists", null, nextRun);

        // :: Assert
        assertEquals(1, firstInsert);
        assertEquals(0, secondInsert);
        Optional<ScheduleDto> schedule = schedulerRep.getSchedule("alreadyExists");
        assertTrue(schedule.isPresent());
        assertEquals(nextRun, schedule.get().getNextRun());
        assertNull(schedule.get().getOverriddenCronExpression());
    }

    @Test
    public void updateNextRun() {
        // :: Setup
        ScheduledTaskRepository schedulerRep = new ScheduledTaskRepository(_dataSource, _clock);
        LocalDateTime insertTime = LocalDateTime.of(2021, 3, 3, 12, 1);
        _clock.setFixedClock(insertTime);
        Instant initialNextRun = getInstant(2021, 3, 3, 12, 24);
        int insertSchedule = schedulerRep.createSchedule("test-schedule", null, initialNextRun);

        // :: Act
        LocalDateTime updateTime = LocalDateTime.of(2021, 3, 3, 12, 12);
        _clock.setFixedClock(updateTime);
        Instant newNextRun = getInstant(2021, 3, 4, 13, 26);
        schedulerRep.updateNextRun("test-schedule",
                CronExpression.parse("0 2 23 ? * *"), newNextRun);

        // :: Assert
        assertEquals(1, insertSchedule);
        Optional<ScheduleDto> schedule = schedulerRep.getSchedule("test-schedule");
        assertTrue(schedule.isPresent());
        assertEquals(newNextRun, schedule.get().getNextRun());
        assertEquals("0 2 23 ? * *", schedule.get().getOverriddenCronExpression().toString());
        assertEquals(updateTime.atZone(ZoneId.systemDefault()).toInstant(), schedule.get().getLastUpdated());
    }

    private Instant getInstant(int year, int month, int dayOfMonth, int hour, int minute) {
        LocalDateTime nextRunTime = LocalDateTime.of(year, month, dayOfMonth, hour, minute);
        return nextRunTime.atZone(ZoneId.systemDefault()).toInstant();
    }

    @Test
    public void setActive() {
        // :: Setup
        ScheduledTaskRepository schedulerRep = new ScheduledTaskRepository(_dataSource, _clock);
        LocalDateTime insertTime = LocalDateTime.of(2021, 3, 3, 12, 1);
        _clock.setFixedClock(insertTime);
        Instant initialNextRun = getInstant(2021, 3, 3, 12, 24);
        schedulerRep.createSchedule("test-schedule", CronExpression.parse("0 2 23 ? * *"), initialNextRun);

        // :: Act
        LocalDateTime updateTime = LocalDateTime.of(2021, 3, 3, 12, 12);
        _clock.setFixedClock(updateTime);
        Optional<ScheduleDto> beforeSettingInactive = schedulerRep.getSchedule("test-schedule");
        schedulerRep.setActive("test-schedule", false);

        // :: Assert
        assertTrue(beforeSettingInactive.isPresent());
        Optional<ScheduleDto> afterSetInactive = schedulerRep.getSchedule("test-schedule");
        assertTrue(afterSetInactive.isPresent());
        assertTrue(beforeSettingInactive.get().isActive());
        assertFalse(afterSetInactive.get().isActive());
        assertEquals("0 2 23 ? * *", beforeSettingInactive.get().getOverriddenCronExpression().toString());
        Instant insertTimeInstant = insertTime.atZone(ZoneId.systemDefault()).toInstant();
        assertEquals(insertTimeInstant, beforeSettingInactive.get().getLastUpdated());
        assertEquals("0 2 23 ? * *", afterSetInactive.get().getOverriddenCronExpression().toString());
        assertEquals(insertTimeInstant, afterSetInactive.get().getLastUpdated());
    }

    @Test
    public void setRunOnce() {
        // :: Setup
        ScheduledTaskRepository schedulerRep = new ScheduledTaskRepository(_dataSource, _clock);
        LocalDateTime insertTime = LocalDateTime.of(2021, 3, 3, 12, 1);
        _clock.setFixedClock(insertTime);
        Instant initialNextRun = getInstant(2021, 3, 3, 12, 24);
        schedulerRep.createSchedule("test-schedule", CronExpression.parse("0 2 23 ? * *"), initialNextRun);

        // :: Act
        LocalDateTime updateTime = LocalDateTime.of(2021, 3, 3, 12, 12);
        _clock.setFixedClock(updateTime);
        Optional<ScheduleDto> beforeSettingRunOnce = schedulerRep.getSchedule("test-schedule");
        schedulerRep.setRunOnce("test-schedule", true);

        // :: Assert
        assertTrue(beforeSettingRunOnce.isPresent());
        Optional<ScheduleDto> afterSetRunOnce = schedulerRep.getSchedule("test-schedule");
        assertTrue(afterSetRunOnce.isPresent());
        assertFalse(beforeSettingRunOnce.get().isRunOnce());
        assertTrue(afterSetRunOnce.get().isRunOnce());
        assertEquals("0 2 23 ? * *", beforeSettingRunOnce.get().getOverriddenCronExpression().toString());
        Instant insertTimeInstant = insertTime.atZone(ZoneId.systemDefault()).toInstant();
        assertEquals(insertTimeInstant, beforeSettingRunOnce.get().getLastUpdated());
        assertEquals("0 2 23 ? * *", afterSetRunOnce.get().getOverriddenCronExpression().toString());
        assertEquals(insertTimeInstant, afterSetRunOnce.get().getLastUpdated());
    }

    @Test
    public void getSchedules() {
        // :: Setup
        ScheduledTaskRepository schedulerRep = new ScheduledTaskRepository(_dataSource, _clock);
        LocalDateTime now = LocalDateTime.of(2021, 3, 3, 12, 1);
        _clock.setFixedClock(now);
        Instant initialNextRun = getInstant(2021, 3, 3, 12, 24);
        schedulerRep.createSchedule("test-schedule-1", null, initialNextRun);
        schedulerRep.createSchedule("test-schedule-2", null, initialNextRun);

        // :: Act
        List<ScheduleDto> schedules = schedulerRep.getSchedules();

        // :: Assert
        assertEquals(2, schedules.size());
        Optional<ScheduleDto> schedule1 = schedules.stream()
                .filter(scheduleDbo -> "test-schedule-1".equalsIgnoreCase(scheduleDbo.getScheduleName()))
                .findAny();
        assertTrue(schedule1.isPresent());
        assertEquals(now.atZone(ZoneId.systemDefault()).toInstant(), schedule1.get().getLastUpdated());
    }

    // ==== Scheduled runs tests =================================================================

    @Test
    public void addScheduleRun_ok() {
        // :: Setup
        ScheduledTaskRepository schedulerRep = new ScheduledTaskRepository(_dataSource, _clock);
        LocalDateTime now = LocalDateTime.of(2021, 3, 3, 12, 1);
        Instant nowInstant = now.atZone(ZoneId.systemDefault()).toInstant();
        _clock.setFixedClock(now);

        // :: Act
        boolean inserted = schedulerRep.addScheduleRun("test-schedule", "some-instance-id",
                nowInstant, "schedule run inserted");

        // :: Assert
        assertTrue(inserted);
        Optional<ScheduledRunDbo> scheduleRun = schedulerRep.getScheduleRunWithLogs("some-instance-id");
        assertTrue(scheduleRun.isPresent());
        assertEquals("schedule run inserted", scheduleRun.get().getStatusMsg());
        assertNull(scheduleRun.get().getStatusThrowable());
        assertEquals(nowInstant, scheduleRun.get().getRunStart());
        assertEquals(0, scheduleRun.get().getLogEntries().size());
        assertEquals(nowInstant, scheduleRun.get().getStatusTime());
        assertEquals(State.STARTED, scheduleRun.get().getStatus());
    }

    @Test
    public void addScheduleRunSameInstanceId_fail() {
        // :: Setup
        ScheduledTaskRepository schedulerRep = new ScheduledTaskRepository(_dataSource, _clock);
        LocalDateTime now = LocalDateTime.of(2021, 3, 3, 12, 1);
        Instant nowInstant = now.atZone(ZoneId.systemDefault()).toInstant();
        _clock.setFixedClock(now);

        // :: Act
        boolean initialInsert = schedulerRep.addScheduleRun("test-schedule", "some-instance-id",
                nowInstant, "schedule run inserted");
        boolean insertFailed = schedulerRep.addScheduleRun("test-schedule-2", "some-instance-id",
                nowInstant, "Should not be inserted");

        // :: Assert
        assertTrue(initialInsert);
        assertFalse(insertFailed);
        Optional<ScheduledRunDbo> scheduleRun = schedulerRep.getScheduleRunWithLogs("some-instance-id");
        assertTrue(scheduleRun.isPresent());
        assertEquals("schedule run inserted", scheduleRun.get().getStatusMsg());
        assertNull(scheduleRun.get().getStatusThrowable());
        assertEquals(nowInstant, scheduleRun.get().getRunStart());
        assertEquals(0, scheduleRun.get().getLogEntries().size());
        assertEquals(nowInstant, scheduleRun.get().getStatusTime());
        assertEquals(State.STARTED, scheduleRun.get().getStatus());
    }

    @Test
    public void setStatusDone_ok() {
        // :: Setup
        ScheduledTaskRepository schedulerRep = new ScheduledTaskRepository(_dataSource, _clock);
        LocalDateTime now = LocalDateTime.of(2021, 3, 3, 12, 1);
        Instant nowInstant = now.atZone(ZoneId.systemDefault()).toInstant();
        _clock.setFixedClock(now);
        boolean inserted = schedulerRep.addScheduleRun("test-schedule", "some-instance-id",
                nowInstant, "schedule run inserted");

        // :: Act
        schedulerRep.setStatus("some-instance-id", State.DONE, "Updating state to DONE",
                null, Instant.now(_clock));

        // :: Assert
        assertTrue(inserted);
        Optional<ScheduledRunDbo> scheduleRun = schedulerRep.getScheduleRunWithLogs("some-instance-id");
        assertTrue(scheduleRun.isPresent());
        assertEquals("Updating state to DONE", scheduleRun.get().getStatusMsg());
        assertEquals(State.DONE, scheduleRun.get().getStatus());
        assertNull(scheduleRun.get().getStatusThrowable());
        assertEquals(nowInstant, scheduleRun.get().getRunStart());
        assertEquals(0, scheduleRun.get().getLogEntries().size());
        assertEquals(nowInstant, scheduleRun.get().getStatusTime());
    }

    @Test
    public void setStatusFail_ok() {
        // :: Setup
        ScheduledTaskRepository schedulerRep = new ScheduledTaskRepository(_dataSource, _clock);
        LocalDateTime now = LocalDateTime.of(2021, 3, 3, 12, 1);
        Instant nowInstant = now.atZone(ZoneId.systemDefault()).toInstant();
        _clock.setFixedClock(now);
        boolean inserted = schedulerRep.addScheduleRun("test-schedule", "some-instance-id",
                nowInstant, "schedule run inserted");

        // :: Act
        schedulerRep.setStatus("some-instance-id", State.FAILED, "This run failed",
                null, Instant.now(_clock));

        // :: Assert
        assertTrue(inserted);
        Optional<ScheduledRunDbo> scheduleRun = schedulerRep.getScheduleRunWithLogs("some-instance-id");
        assertTrue(scheduleRun.isPresent());
        assertEquals("This run failed", scheduleRun.get().getStatusMsg());
        assertEquals(State.FAILED, scheduleRun.get().getStatus());
        assertNull(scheduleRun.get().getStatusThrowable());
        assertEquals(nowInstant, scheduleRun.get().getRunStart());
        assertEquals(0, scheduleRun.get().getLogEntries().size());
        assertEquals(nowInstant, scheduleRun.get().getStatusTime());
    }

    @Test
    public void setStatusFailWithThrowable_ok() {
        // :: Setup
        ScheduledTaskRepository schedulerRep = new ScheduledTaskRepository(_dataSource, _clock);
        LocalDateTime now = LocalDateTime.of(2021, 3, 3, 12, 1);
        Instant nowInstant = now.atZone(ZoneId.systemDefault()).toInstant();
        _clock.setFixedClock(now);
        boolean inserted = schedulerRep.addScheduleRun("test-schedule", "some-instance-id",
                nowInstant, "schedule run inserted");

        // :: Act
        schedulerRep.setStatus("some-instance-id", State.FAILED, "This run failed",
                "some exception", Instant.now(_clock));

        // :: Assert
        assertTrue(inserted);
        Optional<ScheduledRunDbo> scheduleRun = schedulerRep.getScheduleRunWithLogs("some-instance-id");
        assertTrue(scheduleRun.isPresent());
        assertEquals("This run failed", scheduleRun.get().getStatusMsg());
        assertEquals(State.FAILED, scheduleRun.get().getStatus());
        assertNotNull(scheduleRun.get().getStatusThrowable());
        assertEquals(nowInstant, scheduleRun.get().getRunStart());
        assertEquals(0, scheduleRun.get().getLogEntries().size());
        assertEquals(nowInstant, scheduleRun.get().getStatusTime());
    }

    @Test
    public void setFailedSecondTime_fail() {
        // :: Setup
        ScheduledTaskRepository schedulerRep = new ScheduledTaskRepository(_dataSource, _clock);
        LocalDateTime now = LocalDateTime.of(2021, 3, 3, 12, 1);
        Instant nowInstant = now.atZone(ZoneId.systemDefault()).toInstant();
        _clock.setFixedClock(now);
        boolean inserted = schedulerRep.addScheduleRun("test-schedule", "some-instance-id",
                nowInstant, "schedule run inserted");

        // :: Act
        boolean setFailed1 = schedulerRep.setStatus("some-instance-id", State.FAILED,
                "Fault added after insert", "testing failed exception", Instant.now(_clock));
        boolean setFailed2 = schedulerRep.setStatus("some-instance-id", State.FAILED,
                "Fault added a second time", "testing 2 exception", Instant.now(_clock));

        // :: Assert
        assertTrue(inserted);
        assertTrue(setFailed1);
        // Should be no limit on changing end state, IE we are allowed to set the same state multiple times.
        assertTrue(setFailed2);
        Optional<ScheduledRunDbo> scheduleRun = schedulerRep.getScheduleRunWithLogs("some-instance-id");
        assertTrue(scheduleRun.isPresent());
        assertEquals("Fault added a second time", scheduleRun.get().getStatusMsg());
        assertEquals("testing 2 exception", scheduleRun.get().getStatusThrowable());
        assertEquals(nowInstant, scheduleRun.get().getRunStart());
        assertEquals(0, scheduleRun.get().getLogEntries().size());
        assertEquals(nowInstant, scheduleRun.get().getStatusTime());
        assertEquals(State.FAILED, scheduleRun.get().getStatus());
    }

    @Test
    public void setFailedToDone_Ok() {
        // :: Setup
        ScheduledTaskRepository schedulerRep = new ScheduledTaskRepository(_dataSource, _clock);
        LocalDateTime now = LocalDateTime.of(2021, 3, 3, 12, 1);
        Instant nowInstant = now.atZone(ZoneId.systemDefault()).toInstant();
        _clock.setFixedClock(now);
        boolean inserted = schedulerRep.addScheduleRun("test-schedule", "some-instance-id",
                nowInstant, "schedule run inserted");
        boolean setFailed = schedulerRep.setStatus("some-instance-id", State.FAILED,
                "fault added after insert", "testing failed exception", Instant.now(_clock));

        // :: Act
        boolean setDone = schedulerRep.setStatus("some-instance-id", State.DONE,
                "Updated status from failed to done", "second testing failed exception",
                Instant.now(_clock));

        // :: Assert
        assertTrue(inserted);
        assertTrue(setFailed);
        // Should be no limit on changing end state
        assertTrue(setDone);
        Optional<ScheduledRunDbo> scheduleRun = schedulerRep.getScheduleRunWithLogs("some-instance-id");
        assertTrue(scheduleRun.isPresent());
        assertEquals("Updated status from failed to done", scheduleRun.get().getStatusMsg());
        assertEquals("second testing failed exception", scheduleRun.get().getStatusThrowable());
        assertEquals(nowInstant, scheduleRun.get().getRunStart());
        assertEquals(0, scheduleRun.get().getLogEntries().size());
        assertEquals(nowInstant, scheduleRun.get().getStatusTime());
        assertEquals(State.DONE, scheduleRun.get().getStatus());
    }

    @Test
    public void setDoneToFail_ok() {
        // :: Setup
        ScheduledTaskRepository schedulerRep = new ScheduledTaskRepository(_dataSource, _clock);
        LocalDateTime now = LocalDateTime.of(2021, 3, 3, 12, 1);
        Instant nowInstant = now.atZone(ZoneId.systemDefault()).toInstant();
        _clock.setFixedClock(now);
        boolean inserted = schedulerRep.addScheduleRun("test-schedule", "some-instance-id",
                nowInstant, "schedule run inserted");
        boolean setDone = schedulerRep.setStatus("some-instance-id", State.DONE,
                "done set after insert", null, Instant.now(_clock));

        // :: Act
        boolean setFailed = schedulerRep.setStatus("some-instance-id", State.FAILED,
                "Updated status from done to failed", "testing failed exception", Instant.now(_clock));


        // :: Assert
        assertTrue(inserted);
        assertTrue(setDone);
        // Should be no limit on changing end state
        assertTrue(setFailed);
        Optional<ScheduledRunDbo> scheduleRun = schedulerRep.getScheduleRunWithLogs("some-instance-id");
        assertTrue(scheduleRun.isPresent());
        assertEquals("Updated status from done to failed", scheduleRun.get().getStatusMsg());
        assertEquals("testing failed exception", scheduleRun.get().getStatusThrowable());
        assertEquals(nowInstant, scheduleRun.get().getRunStart());
        assertEquals(0, scheduleRun.get().getLogEntries().size());
        assertEquals(nowInstant, scheduleRun.get().getStatusTime());
        assertEquals(State.FAILED, scheduleRun.get().getStatus());
    }

    @Test
    public void setDispatchedToFail_ok() {
        // :: Setup
        ScheduledTaskRepository schedulerRep = new ScheduledTaskRepository(_dataSource, _clock);
        LocalDateTime now = LocalDateTime.of(2021, 3, 3, 12, 1);
        Instant nowInstant = now.atZone(ZoneId.systemDefault()).toInstant();
        _clock.setFixedClock(now);
        boolean inserted = schedulerRep.addScheduleRun("test-schedule", "some-instance-id",
                nowInstant, "schedule run inserted");
        boolean setDispatched = schedulerRep.setStatus("some-instance-id", State.DISPATCHED,
                "Dispatch set after insert", null, Instant.now(_clock));

        // :: Act
        LocalDateTime failTime = LocalDateTime.of(2021, 3, 3, 12, 2);
        Instant failInstant = failTime.atZone(ZoneId.systemDefault()).toInstant();
        _clock.setFixedClock(failTime);
        boolean setFailed = schedulerRep.setStatus("some-instance-id", State.FAILED,
                "Dispatched to fail is ok", "testing failed exception", Instant.now(_clock));


        // :: Assert
        assertTrue(inserted);
        assertTrue(setDispatched);
        assertTrue(setFailed);
        Optional<ScheduledRunDbo> scheduleRun = schedulerRep.getScheduleRunWithLogs("some-instance-id");
        assertTrue(scheduleRun.isPresent());
        assertEquals("Dispatched to fail is ok", scheduleRun.get().getStatusMsg());
        assertEquals("testing failed exception",
                scheduleRun.get().getStatusThrowable());
        assertEquals(nowInstant, scheduleRun.get().getRunStart());
        assertEquals(0, scheduleRun.get().getLogEntries().size());
        assertEquals(failInstant, scheduleRun.get().getStatusTime());
        assertEquals(State.FAILED, scheduleRun.get().getStatus());
    }

    @Test
    public void setDispatchedToDispatchedToDone_ok() {
        // :: Setup
        ScheduledTaskRepository schedulerRep = new ScheduledTaskRepository(_dataSource, _clock);
        LocalDateTime now = LocalDateTime.of(2021, 3, 3, 12, 1);
        Instant nowInstant = now.atZone(ZoneId.systemDefault()).toInstant();
        _clock.setFixedClock(now);
        boolean inserted = schedulerRep.addScheduleRun("test-schedule", "some-instance-id",
                nowInstant, "schedule run inserted");
        boolean firstDispatched = schedulerRep.setStatus("some-instance-id", State.DISPATCHED,
                "Dispatch set after insert", null, Instant.now(_clock));

        // :: Act
        _clock.setFixedClock(LocalDateTime.of(2021, 3, 3, 12, 2));
        boolean secondDispatched = schedulerRep.setStatus("some-instance-id", State.DISPATCHED,
                "Second Dispatch", null, Instant.now(_clock));
        LocalDateTime doneTime = LocalDateTime.of(2021, 3, 3, 12, 2);
        Instant doneInstant = doneTime.atZone(ZoneId.systemDefault()).toInstant();
        _clock.setFixedClock(doneTime);
        boolean setDone = schedulerRep.setStatus("some-instance-id", State.DONE,
                "Dispatched to done is ok", null, Instant.now(_clock));

        // :: Assert
        assertTrue(inserted);
        assertTrue(firstDispatched);
        assertTrue(secondDispatched);
        assertTrue(setDone);
        Optional<ScheduledRunDbo> scheduleRun = schedulerRep.getScheduleRunWithLogs("some-instance-id");
        assertTrue(scheduleRun.isPresent());
        assertEquals("Dispatched to done is ok", scheduleRun.get().getStatusMsg());
        assertNull(scheduleRun.get().getStatusThrowable());
        assertEquals(nowInstant, scheduleRun.get().getRunStart());
        assertEquals(0, scheduleRun.get().getLogEntries().size());
        assertEquals(doneInstant, scheduleRun.get().getStatusTime());
        assertEquals(State.DONE, scheduleRun.get().getStatus());
    }

    @Test
    public void addLogEntry_ok() {
        // :: Setup
        ScheduledTaskRepository schedulerRep = new ScheduledTaskRepository(_dataSource, _clock);
        LocalDateTime now = LocalDateTime.of(2021, 3, 3, 12, 1);
        Instant nowInstant = now.atZone(ZoneId.systemDefault()).toInstant();
        _clock.setFixedClock(now);
        schedulerRep.addScheduleRun("test-schedule", "some-instance-id",
                nowInstant, "schedule run inserted");

        // :: Act
        LocalDateTime logTime = LocalDateTime.of(2021, 3, 3, 12, 2);
        LogEntry logEntry = new LogEntryImpl("some log message", logTime);
        int addCount = schedulerRep.addLogEntry("some-instance-id", logEntry);

        // :: Assert
        assertEquals(1, addCount);
        List<LogEntry> logEntryFromDb = schedulerRep.getLogEntries("some-instance-id");
        assertEquals(1, logEntryFromDb.size());
        assertEquals("some log message", logEntryFromDb.get(0).getMessage());
        assertFalse(logEntryFromDb.get(0).getThrowable().isPresent());
        assertEquals(logTime, logEntryFromDb.get(0).getLogTime());
    }

    @Test
    public void addLogEntryWithThrowable_ok() {
        // :: Setup
        ScheduledTaskRepository schedulerRep = new ScheduledTaskRepository(_dataSource, _clock);
        LocalDateTime now = LocalDateTime.of(2021, 3, 3, 12, 1);
        Instant nowInstant = now.atZone(ZoneId.systemDefault()).toInstant();
        _clock.setFixedClock(now);
        schedulerRep.addScheduleRun("test-schedule", "some-instance-id",
                nowInstant, "schedule run inserted");

        // :: Act
        LogEntry logEntry = new LogEntryImpl("some log message", "testing throwable", now);
        int addCount = schedulerRep.addLogEntry("some-instance-id", logEntry);

        // :: Assert
        assertEquals(1, addCount);
        List<LogEntry> logEntryFromDb = schedulerRep.getLogEntries("some-instance-id");
        assertEquals(1, logEntryFromDb.size());
        LogEntry firstLogEntry = logEntryFromDb.get(0);
        assertEquals("some log message", firstLogEntry.getMessage());
        assertEquals("testing throwable", firstLogEntry.getThrowable().get());
        assertEquals(now, firstLogEntry.getLogTime());
    }

    @Test
    public void getLastRunForSchedule_ok() {
        // :: Setup
        ScheduledTaskRepository schedulerRep = new ScheduledTaskRepository(_dataSource, _clock);
        _clock.setFixedClock(LocalDateTime.of(2021, 3, 3, 12, 1));

        // first - test-schedule-1
        schedulerRep.addScheduleRun("test-schedule-1", "instance-id-firs-run",
                LocalDateTime.of(2021, 3, 3, 12, 1).atZone(ZoneId.systemDefault()).toInstant(),
                "first run schedule 1");
        LogEntry logEntry = new LogEntryImpl("some log message",
                LocalDateTime.of(2021, 3, 3, 12, 2));
        schedulerRep.addLogEntry("instance-id-firs-run", logEntry);

        // second - test-schedule-2
        _clock.setFixedClock(LocalDateTime.of(2021, 3, 3, 12, 10));
        schedulerRep.addScheduleRun("test-schedule-2", "instance-id-second-run",
                LocalDateTime.of(2021, 3, 3, 12, 10).atZone(ZoneId.systemDefault()).toInstant(),
                "first run schedule 2");
        schedulerRep.addLogEntry("instance-id-second-run", new LogEntryImpl("second schedule log entry",
                LocalDateTime.of(2021, 3, 3, 12, 12)));

        // third - test-schedule-1 - this is one we want to get
        LocalDateTime lastInsertTime = LocalDateTime.of(2021, 3, 3, 12, 15);
        _clock.setFixedClock(lastInsertTime);
        schedulerRep.addScheduleRun("test-schedule-1", "instance-id-thirds-run",
                LocalDateTime.of(2021, 3, 3, 12, 15).atZone(ZoneId.systemDefault()).toInstant(),
                "second run schedule 1");
        LocalDateTime lastLogTime = LocalDateTime.of(2021, 3, 3, 12, 16);
        schedulerRep.addLogEntry("instance-id-thirds-run", new LogEntryImpl("first schedule log entry 2",
                lastLogTime));

        // forth - test-schedule-2
        _clock.setFixedClock(LocalDateTime.of(2021, 3, 3, 12, 20));
        schedulerRep.addScheduleRun("test-schedule-2", "instance-id-forth-run",
                LocalDateTime.of(2021, 3, 3, 12, 20).atZone(ZoneId.systemDefault()).toInstant(),
                "second run schedule 2");
        schedulerRep.addLogEntry("instance-id-forth-run", new LogEntryImpl("second schedule log entry 2",
                LocalDateTime.of(2021, 3, 3, 12, 21)));

        // :: Act
        Optional<ScheduledRunDto> scheduleRunFromDb = schedulerRep.getLastRunForSchedule("test-schedule-1");

        // :: Assert
        assertTrue(scheduleRunFromDb.isPresent());
        assertEquals(lastInsertTime, scheduleRunFromDb.get().getRunStart());
        assertEquals("instance-id-thirds-run", scheduleRunFromDb.get().getInstanceId());
    }

    @Test
    public void getScheduleRunWithLogssBetween_ok() {
        // :: Setup
        ScheduledTaskRepository schedulerRep = new ScheduledTaskRepository(_dataSource, _clock);
        _clock.setFixedClock(LocalDateTime.of(2021, 3, 3, 12, 1));

        // first - test-schedule-1 - this should not be included by the get
        schedulerRep.addScheduleRun("test-schedule-1", "instance-id-firs-run",
                LocalDateTime.of(2021, 3, 3, 12, 1).atZone(ZoneId.systemDefault()).toInstant(),
                "first run schedule 1");
        LogEntry logEntry = new LogEntryImpl("some log message",
                LocalDateTime.of(2021, 3, 3, 12, 2));
        schedulerRep.addLogEntry("instance-id-firs-run", logEntry);

        // second - test-schedule-1
        _clock.setFixedClock(LocalDateTime.of(2021, 3, 3, 12, 10));
        schedulerRep.addScheduleRun("test-schedule-1", "instance-id-second-run",
                LocalDateTime.of(2021, 3, 3, 12, 10).atZone(ZoneId.systemDefault()).toInstant(),
                "second run schedule 1");
        schedulerRep.addLogEntry("instance-id-second-run", new LogEntryImpl("first schedule log entry 2",
                LocalDateTime.of(2021, 3, 3, 12, 12)));

        // third - test-schedule-1 - get from including
        _clock.setFixedClock(LocalDateTime.of(2021, 3, 3, 12, 15));
        schedulerRep.addScheduleRun("test-schedule-1", "instance-id-third-run",
                LocalDateTime.of(2021, 3, 3, 12, 15).atZone(ZoneId.systemDefault()).toInstant(),
                "third run schedule 1");
        _clock.setFixedClock(LocalDateTime.of(2021, 3, 3, 12, 16));
        schedulerRep.addLogEntry("instance-id-third-run", new LogEntryImpl("first schedule log entry 3",
                LocalDateTime.of(2021, 3, 3, 12, 16)));

        // forth - test-schedule-2 - this should not be picked up due to it is another scheduleName
        _clock.setFixedClock(LocalDateTime.of(2021, 3, 3, 12, 20));
        schedulerRep.addScheduleRun("test-schedule-2", "instance-id-forth-run",
                LocalDateTime.of(2021, 3, 3, 12, 20).atZone(ZoneId.systemDefault()).toInstant(),
                "first run schedule 2");
        schedulerRep.addLogEntry("instance-id-forth-run", new LogEntryImpl("second schedule log entry 1",
                LocalDateTime.of(2021, 3, 3, 12, 20)));

        // fifth - test-schedule-1 - get to including
        _clock.setFixedClock(LocalDateTime.of(2021, 3, 3, 12, 25));
        schedulerRep.addScheduleRun("test-schedule-1", "instance-id-fifth-run",
                                LocalDateTime.of(2021, 3, 3, 12, 25).atZone(ZoneId.systemDefault()).toInstant(),
                "forth run schedule 1");
        schedulerRep.addLogEntry("instance-id-fifth-run", new LogEntryImpl("first schedule log entry 4",
                LocalDateTime.of(2021, 3, 3, 12, 25)));

        // sixth - test-schedule-1 - should not be included
        _clock.setFixedClock(LocalDateTime.of(2021, 3, 3, 12, 30));
        schedulerRep.addScheduleRun("test-schedule-1", "instance-id-sixth-run",
                LocalDateTime.of(2021, 3, 3, 12, 30).atZone(ZoneId.systemDefault()).toInstant(),
                "fifth run schedule 1");
        schedulerRep.addLogEntry("instance-id-sixth-run", new LogEntryImpl("first schedule log entry 5",
                LocalDateTime.of(2021, 3, 3, 12, 30)));

        // :: Act
        List<ScheduledRunDto> scheduleRunFromDb = schedulerRep.getScheduleRunsBetween(
                "test-schedule-1",
                LocalDateTime.of(2021, 3, 3, 12, 12),
                LocalDateTime.of(2021, 3, 3, 12, 26));

        // :: Assert
        assertEquals(2, scheduleRunFromDb.size());
        List<LogEntry> logEntriesFromDb1 = schedulerRep.getLogEntries(scheduleRunFromDb.get(0).getInstanceId());
        assertEquals(1, logEntriesFromDb1.size());
        // Get the second schedule's logEntries
        List<LogEntry> logEntriesFromDb2 = schedulerRep.getLogEntries(scheduleRunFromDb.get(1).getInstanceId());
        assertEquals("first schedule log entry 3", logEntriesFromDb1.get(0).getMessage());
        assertEquals("first schedule log entry 4", logEntriesFromDb2.get(0).getMessage());
        assertFalse(logEntriesFromDb1.get(0).getThrowable().isPresent());
        // Show that the runTime can differ from the logTime
        assertEquals(LocalDateTime.of(2021, 3, 3, 12, 15),
                scheduleRunFromDb.get(0).getRunStart());
        assertEquals(LocalDateTime.of(2021, 3, 3, 12, 16),
                logEntriesFromDb1.get(0).getLogTime());
        assertFalse(logEntriesFromDb2.get(0).getThrowable().isPresent());

    }


    @Test
    public void getLastScheduleRuns_ok() {
        // :: Setup
        ScheduledTaskRepository schedulerRep = new ScheduledTaskRepository(_dataSource, _clock);
        _clock.setFixedClock(LocalDateTime.of(2021, 3, 3, 12, 1));

        // first - test-schedule-1 - this should not be included by the get
        schedulerRep.addScheduleRun("test-schedule-1", "instance-id-firs-run",
                LocalDateTime.of(2021, 3, 3, 12, 1).atZone(ZoneId.systemDefault()).toInstant(),
                "first run schedule 1");
        LogEntry logEntry = new LogEntryImpl("some log message",
                LocalDateTime.of(2021, 3, 3, 12, 2));
        schedulerRep.addLogEntry("instance-id-firs-run", logEntry);

        // second - test-schedule-1
        schedulerRep.addScheduleRun("test-schedule-1", "instance-id-second-run",
                LocalDateTime.of(2021, 3, 3, 12, 10).atZone(ZoneId.systemDefault()).toInstant(),
                "second run schedule 1");
        schedulerRep.addLogEntry("instance-id-second-run", new LogEntryImpl("first schedule log entry 2",
                LocalDateTime.of(2021, 3, 3, 12, 10)));

        // third - test-schedule-1
        schedulerRep.addScheduleRun("test-schedule-1", "instance-id-third-run",
                LocalDateTime.of(2021, 3, 3, 12, 15).atZone(ZoneId.systemDefault()).toInstant(),
                "third run schedule 1");
        schedulerRep.addLogEntry("instance-id-third-run", new LogEntryImpl("first schedule log entry 3",
                LocalDateTime.of(2021, 3, 3, 12, 16)));

        // forth - test-schedule-2 - first from schedule 2
        schedulerRep.addScheduleRun("test-schedule-2", "instance-id-forth-run",
                LocalDateTime.of(2021, 3, 3, 12, 20).atZone(ZoneId.systemDefault()).toInstant(),
                "first run schedule 2");
        schedulerRep.addLogEntry("instance-id-forth-run", new LogEntryImpl("second schedule log entry 1",
                LocalDateTime.of(2021, 3, 3, 12, 20)));

        // fifth - test-schedule-1
        _clock.setFixedClock(LocalDateTime.of(2021, 3, 3, 12, 25));
        schedulerRep.addScheduleRun("test-schedule-1", "instance-id-fifth-run",
                LocalDateTime.of(2021, 3, 3, 12, 25).atZone(ZoneId.systemDefault()).toInstant(),
                "forth run schedule 1");
        schedulerRep.addLogEntry("instance-id-fifth-run", new LogEntryImpl("first schedule log entry 4",
                LocalDateTime.of(2021, 3, 3, 12, 25)));

        // sixth - test-schedule-1 - should should be last run done and the one we want to retrieve
        LocalDateTime lastRunTimeSchedule1 = LocalDateTime.of(2021, 3, 3, 12, 30);
        schedulerRep.addScheduleRun("test-schedule-1", "instance-id-sixth-run",
                lastRunTimeSchedule1.atZone(ZoneId.systemDefault()).toInstant(),
                "fifth run schedule 1");
        schedulerRep.addLogEntry("instance-id-sixth-run", new LogEntryImpl("first schedule log entry 5",
                LocalDateTime.of(2021, 3, 3, 12, 31)));

        // seventh - schedule-2 - this is the last run for the schedule 2 that we want to retrieve
        LocalDateTime lastRunTimeSchedule2 = LocalDateTime.of(2021, 3, 3, 12, 32);
        schedulerRep.addScheduleRun("test-schedule-2", "instance-id-seventh-run",
                lastRunTimeSchedule2.atZone(ZoneId.systemDefault()).toInstant(),
                "second run schedule 2");
        schedulerRep.addLogEntry("instance-id-seventh-run", new LogEntryImpl("second schedule log entry 2",
                LocalDateTime.of(2021, 3, 3, 12, 33)));

        // :: Act
        List<ScheduledRunDto> scheduleRunFromDb = schedulerRep.getLastScheduleRuns();

        // :: Assert
        assertEquals(2, scheduleRunFromDb.size());
        // We should have the last run first, IE the second schedule entry 2.

        // Show that the runTime can differ from the logTime
        assertEquals(lastRunTimeSchedule2, scheduleRunFromDb.get(0).getRunStart());
    }


}