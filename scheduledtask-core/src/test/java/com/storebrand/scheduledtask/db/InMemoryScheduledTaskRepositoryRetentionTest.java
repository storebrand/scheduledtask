package com.storebrand.scheduledtask.db;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.storebrand.scheduledtask.ScheduledTask.RetentionPolicy;
import com.storebrand.scheduledtask.ScheduledTaskConfig.StaticRetentionPolicy;
import com.storebrand.scheduledtask.ScheduledTaskRegistry.State;
import com.storebrand.scheduledtask.db.InMemoryMasterLockRepositoryTest.ClockMock;
import com.storebrand.scheduledtask.db.ScheduledTaskRepository.ScheduledRunDto;

/**
 * Test that retention policies keep the correct runs as specified in the policy.
 *
 * @author Kristian Hiim
 */
public class InMemoryScheduledTaskRepositoryRetentionTest {
    private final ClockMock _clock = new ClockMock();

    private InMemoryScheduledTaskRepository _repository;

    @BeforeEach
    public void before() {
        _repository = new InMemoryScheduledTaskRepository(_clock);
    }

    @Test
    public void removeOldRunsWithDefaultRetentionPolicy() {
        // :: Setup
        createRunWithStatus("test-schedule", LocalDateTime.of(2021, 1, 1, 1, 1),
                "instance1", State.DONE);
        createRunWithStatus("test-schedule", LocalDateTime.of(2021, 2, 1, 1, 1),
                "instance2", State.FAILED);
        createRunWithStatus("test-schedule", LocalDateTime.of(2021, 3, 1, 1, 1),
                "instance3", State.DONE);

        createRunWithStatus("test-schedule", LocalDateTime.of(2022, 1, 1, 1, 1),
                "instance4", State.DONE);

        // :: Act
        LocalDateTime now = LocalDateTime.of(2022, 4, 1, 1, 1);
        RetentionPolicy retentionPolicy = new StaticRetentionPolicy.Builder()
                .build();
        _clock.setFixedClock(now.atZone(ZoneId.systemDefault()).toInstant());
        _repository.executeRetentionPolicy("test-schedule", retentionPolicy);

        // :: Assert
        List<ScheduledRunDto> scheduledRuns = _repository.getScheduleRunsBetween("test-schedule",
                LocalDateTime.of(2000, 1, 1, 1, 1),
                LocalDateTime.of(3000, 1, 1, 1, 1));
        assertEquals(1, scheduledRuns.size());
        assertEquals("instance4", scheduledRuns.get(0).getHostname());
    }

    @Test
    public void removeOldFailedRunsWithCustomRetentionPolicy() {
        // :: Setup
        createRunWithStatus("test-schedule", LocalDateTime.of(2021, 1, 1, 1, 1),
                "instance1", State.DONE);
        createRunWithStatus("test-schedule", LocalDateTime.of(2021, 2, 1, 1, 1),
                "instance2", State.FAILED);
        createRunWithStatus("test-schedule", LocalDateTime.of(2021, 3, 1, 1, 1),
                "instance3", State.DONE);

        createRunWithStatus("test-schedule", LocalDateTime.of(2022, 1, 1, 1, 1),
                "instance4", State.DONE);

        // :: Act
        LocalDateTime now = LocalDateTime.of(2022, 4, 1, 1, 1);
        RetentionPolicy retentionPolicy = new StaticRetentionPolicy.Builder()
                .deleteFailedRunsAfterDays(365)
                .deleteRunsAfterDays(3000)
                .build();
        _clock.setFixedClock(now.atZone(ZoneId.systemDefault()).toInstant());
        _repository.executeRetentionPolicy("test-schedule", retentionPolicy);

        // :: Assert
        List<ScheduledRunDto> scheduledRuns = _repository.getScheduleRunsBetween("test-schedule",
                LocalDateTime.of(2000, 1, 1, 1, 1),
                LocalDateTime.of(3000, 1, 1, 1, 1));
        assertEquals(3, scheduledRuns.size());
        assertEquals("instance4", scheduledRuns.get(0).getHostname());
        assertEquals("instance3", scheduledRuns.get(1).getHostname());
        assertEquals("instance1", scheduledRuns.get(2).getHostname());
    }

    @Test
    public void removeOldSuccessfulRunsWithCustomRetentionPolicy() {
        // :: Setup
        createRunWithStatus("test-schedule", LocalDateTime.of(2021, 1, 1, 1, 1),
                "instance1", State.DONE);
        createRunWithStatus("test-schedule", LocalDateTime.of(2021, 2, 1, 1, 1),
                "instance2", State.FAILED);
        createRunWithStatus("test-schedule", LocalDateTime.of(2021, 3, 1, 1, 1),
                "instance3", State.DONE);

        createRunWithStatus("test-schedule", LocalDateTime.of(2022, 1, 1, 1, 1),
                "instance4", State.DONE);

        // :: Act
        LocalDateTime now = LocalDateTime.of(2022, 4, 1, 1, 1);
        RetentionPolicy retentionPolicy = new StaticRetentionPolicy.Builder()
                .deleteSuccessfulRunsAfterDays(365)
                .deleteRunsAfterDays(3000)
                .build();
        _clock.setFixedClock(now.atZone(ZoneId.systemDefault()).toInstant());
        _repository.executeRetentionPolicy("test-schedule", retentionPolicy);

        // :: Assert
        List<ScheduledRunDto> scheduledRuns = _repository.getScheduleRunsBetween("test-schedule",
                LocalDateTime.of(2000, 1, 1, 1, 1),
                LocalDateTime.of(3000, 1, 1, 1, 1));
        assertEquals(2, scheduledRuns.size());
        assertEquals("instance4", scheduledRuns.get(0).getHostname());
        assertEquals("instance2", scheduledRuns.get(1).getHostname());
    }


    @Test
    public void keepMaxRunsWithRetentionPolicy() {
        // :: Setup
        createRunWithStatus("test-schedule", LocalDateTime.of(2021, 1, 1, 1, 1),
                "instance1", State.DONE);
        createRunWithStatus("test-schedule", LocalDateTime.of(2021, 2, 1, 1, 1),
                "instance2", State.FAILED);
        createRunWithStatus("test-schedule", LocalDateTime.of(2021, 3, 1, 1, 1),
                "instance3", State.DONE);

        createRunWithStatus("test-schedule", LocalDateTime.of(2022, 1, 1, 1, 1),
                "instance4", State.DONE);

        // :: Act
        LocalDateTime now = LocalDateTime.of(2022, 4, 1, 1, 1);
        RetentionPolicy retentionPolicy = new StaticRetentionPolicy.Builder()
                .deleteRunsAfterDays(0)
                .keepMaxRuns(2)
                .build();
        _clock.setFixedClock(now.atZone(ZoneId.systemDefault()).toInstant());
        _repository.executeRetentionPolicy("test-schedule", retentionPolicy);

        // :: Assert
        List<ScheduledRunDto> scheduledRuns = _repository.getScheduleRunsBetween("test-schedule",
                LocalDateTime.of(2000, 1, 1, 1, 1),
                LocalDateTime.of(3000, 1, 1, 1, 1));
        assertEquals(2, scheduledRuns.size());
        assertEquals("instance4", scheduledRuns.get(0).getHostname());
        assertEquals("instance3", scheduledRuns.get(1).getHostname());
    }

    @Test
    public void keepMaxSuccessfulRunsWithRetentionPolicy() {
        // :: Setup
        createRunWithStatus("test-schedule", LocalDateTime.of(2021, 1, 1, 1, 1),
                "instance1", State.DONE);
        createRunWithStatus("test-schedule", LocalDateTime.of(2021, 2, 1, 1, 1),
                "instance2", State.FAILED);
        createRunWithStatus("test-schedule", LocalDateTime.of(2021, 3, 1, 1, 1),
                "instance3", State.DONE);

        createRunWithStatus("test-schedule", LocalDateTime.of(2022, 1, 1, 1, 1),
                "instance4", State.DONE);

        // :: Act
        LocalDateTime now = LocalDateTime.of(2022, 4, 1, 1, 1);
        RetentionPolicy retentionPolicy = new StaticRetentionPolicy.Builder()
                .deleteRunsAfterDays(3000)
                .keepMaxSuccessfulRuns(1)
                .build();
        _clock.setFixedClock(now.atZone(ZoneId.systemDefault()).toInstant());
        _repository.executeRetentionPolicy("test-schedule", retentionPolicy);

        // :: Assert
        List<ScheduledRunDto> scheduledRuns = _repository.getScheduleRunsBetween("test-schedule",
                LocalDateTime.of(2000, 1, 1, 1, 1),
                LocalDateTime.of(3000, 1, 1, 1, 1));
        assertEquals(2, scheduledRuns.size());
        assertEquals("instance4", scheduledRuns.get(0).getHostname());
        assertEquals("instance2", scheduledRuns.get(1).getHostname());
    }

    @Test
    public void keepMaxFailedRunsWithRetentionPolicy() {
        // :: Setup
        createRunWithStatus("test-schedule", LocalDateTime.of(2021, 1, 1, 1, 1),
                "instance1", State.FAILED);
        createRunWithStatus("test-schedule", LocalDateTime.of(2021, 2, 1, 1, 1),
                "instance2", State.DONE);
        createRunWithStatus("test-schedule", LocalDateTime.of(2021, 3, 1, 1, 1),
                "instance3", State.FAILED);

        createRunWithStatus("test-schedule", LocalDateTime.of(2022, 1, 1, 1, 1),
                "instance4", State.FAILED);

        // :: Act
        LocalDateTime now = LocalDateTime.of(2022, 4, 1, 1, 1);
        RetentionPolicy retentionPolicy = new StaticRetentionPolicy.Builder()
                .deleteRunsAfterDays(3000)
                .keepMaxFailedRuns(1)
                .build();
        _clock.setFixedClock(now.atZone(ZoneId.systemDefault()).toInstant());
        _repository.executeRetentionPolicy("test-schedule", retentionPolicy);

        // :: Assert
        List<ScheduledRunDto> scheduledRuns = _repository.getScheduleRunsBetween("test-schedule",
                LocalDateTime.of(2000, 1, 1, 1, 1),
                LocalDateTime.of(3000, 1, 1, 1, 1));
        assertEquals(2, scheduledRuns.size());
        assertEquals("instance4", scheduledRuns.get(0).getHostname());
        assertEquals("instance2", scheduledRuns.get(1).getHostname());
    }


    private void createRunWithStatus(String scheduleName, LocalDateTime runTime, String hostname, State state) {
        _clock.setFixedClock(runTime);
        long runId = _repository.addScheduleRun(scheduleName, hostname,
                runTime.atZone(ZoneId.systemDefault()).toInstant(), "schedule run inserted");
        _repository.addLogEntry(runId, runTime, "Some log message");
        _repository.setStatus(runId, state, "Hello world", null,
                runTime.atZone(ZoneId.systemDefault()).toInstant());
    }
}
