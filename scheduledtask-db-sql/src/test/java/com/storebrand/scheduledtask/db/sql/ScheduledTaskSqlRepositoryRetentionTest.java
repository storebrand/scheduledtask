package com.storebrand.scheduledtask.db.sql;

import static com.storebrand.scheduledtask.db.sql.ScheduledTaskSqlRepositoryTest.SCHEDULE_LOG_ENTRY_CREATE_SQL;
import static com.storebrand.scheduledtask.db.sql.ScheduledTaskSqlRepositoryTest.SCHEDULE_RUN_CREATE_SQL;
import static com.storebrand.scheduledtask.db.sql.ScheduledTaskSqlRepositoryTest.SCHEDULE_RUN_INDEX_CREATE_SQL;
import static com.storebrand.scheduledtask.db.sql.ScheduledTaskSqlRepositoryTest.STOREBRAND_SCHEDULE_CREATE_SQL;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;

import javax.sql.DataSource;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;

import com.storebrand.scheduledtask.ScheduledTask.RetentionPolicy;
import com.storebrand.scheduledtask.ScheduledTaskConfig.StaticRetentionPolicy;
import com.storebrand.scheduledtask.ScheduledTaskRegistry.State;
import com.storebrand.scheduledtask.db.ScheduledTaskRepository.ScheduledRunDto;
import com.storebrand.scheduledtask.db.sql.MasterLockRepositoryTest.ClockMock;

/**
 * Test that retention policies keep the correct runs as specified in the policy.
 *
 * @author Kristian Hiim
 */
public class ScheduledTaskSqlRepositoryRetentionTest {
    private final JdbcTemplate _jdbcTemplate;
    private final DataSource _dataSource;
    private final ClockMock _clock = new ClockMock();

    private ScheduledTaskSqlRepository _repository;

    public ScheduledTaskSqlRepositoryRetentionTest() {
        _dataSource = new SingleConnectionDataSource("jdbc:h2:mem:testStorebrandSchedulerDbRetention", true);
        _jdbcTemplate = new JdbcTemplate(_dataSource);
    }

    @BeforeEach
    public void before() {
        _jdbcTemplate.execute(STOREBRAND_SCHEDULE_CREATE_SQL);
        _jdbcTemplate.execute(SCHEDULE_RUN_CREATE_SQL);
        _jdbcTemplate.execute(SCHEDULE_RUN_INDEX_CREATE_SQL);
        _jdbcTemplate.execute(SCHEDULE_LOG_ENTRY_CREATE_SQL);

        _repository = new ScheduledTaskSqlRepository(_dataSource, _clock);
    }

    @AfterEach
    public void after() {
        _jdbcTemplate.execute("DROP TABLE " + ScheduledTaskSqlRepository.SCHEDULE_LOG_ENTRY_TABLE + ";");
        _jdbcTemplate.execute("DROP TABLE " + ScheduledTaskSqlRepository.SCHEDULE_RUN_TABLE + ";");
        _jdbcTemplate.execute("DROP TABLE " + ScheduledTaskSqlRepository.SCHEDULE_TASK_TABLE + ";");
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
        assertEquals("instance4", scheduledRuns.get(0).getInstanceId());
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
        assertEquals("instance4", scheduledRuns.get(0).getInstanceId());
        assertEquals("instance3", scheduledRuns.get(1).getInstanceId());
        assertEquals("instance1", scheduledRuns.get(2).getInstanceId());
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
        assertEquals("instance4", scheduledRuns.get(0).getInstanceId());
        assertEquals("instance2", scheduledRuns.get(1).getInstanceId());
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
        assertEquals("instance4", scheduledRuns.get(0).getInstanceId());
        assertEquals("instance3", scheduledRuns.get(1).getInstanceId());
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
        assertEquals("instance4", scheduledRuns.get(0).getInstanceId());
        assertEquals("instance2", scheduledRuns.get(1).getInstanceId());
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
        assertEquals("instance4", scheduledRuns.get(0).getInstanceId());
        assertEquals("instance2", scheduledRuns.get(1).getInstanceId());
    }


    private void createRunWithStatus(String scheduleName, LocalDateTime runTime, String instanceId, State state) {
        _clock.setFixedClock(runTime);
        _repository.addScheduleRun(scheduleName, instanceId,
                runTime.atZone(ZoneId.systemDefault()).toInstant(), "schedule run inserted");
        _repository.addLogEntry(instanceId, runTime, "Some log message");
        _repository.setStatus(instanceId, state, "Hello world", null,
                runTime.atZone(ZoneId.systemDefault()).toInstant());
    }
}
