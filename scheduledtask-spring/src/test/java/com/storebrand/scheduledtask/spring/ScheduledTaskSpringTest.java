package com.storebrand.scheduledtask.spring;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import javax.sql.DataSource;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;

import com.storebrand.scheduledtask.ScheduledTaskRegistry;
import com.storebrand.scheduledtask.db.sql.MasterLockSqlRepository;
import com.storebrand.scheduledtask.db.sql.ScheduledTaskSqlRepository;
import com.storebrand.scheduledtask.spring.testscheduledtasks1.SpringComponentWithScheduledTask;

public class ScheduledTaskSpringTest {

    private final JdbcTemplate _jdbcTemplate;
    private final DataSource _dataSource;

    public ScheduledTaskSpringTest() {
        _dataSource = new SingleConnectionDataSource("jdbc:h2:mem:testStorebrandScheduledTaskSpring", true);
        _jdbcTemplate = new JdbcTemplate(_dataSource);
    }

    @BeforeEach
    public void before() {
        // Database initialization copied from scheduledtask-db-sql unit tests. Should be kept in sync.
        _jdbcTemplate.execute(MASTER_TABLE_CREATE_SQL);
        _jdbcTemplate.execute(SCHEDULE_TABLE_VERSION_CREATE_SQL);
        _jdbcTemplate.execute("INSERT INTO stb_schedule_table_version (version) "
                + " VALUES (1)");
        _jdbcTemplate.execute(STOREBRAND_SCHEDULE_CREATE_SQL);
        _jdbcTemplate.execute(SCHEDULE_RUN_CREATE_SQL);
        _jdbcTemplate.execute(SCHEDULE_RUN_INDEX_CREATE_SQL);
        _jdbcTemplate.execute(SCHEDULE_LOG_ENTRY_CREATE_SQL);
    }

    @AfterEach
    public void after() {
        _jdbcTemplate.execute("DROP TABLE " + ScheduledTaskSqlRepository.SCHEDULE_LOG_ENTRY_TABLE + ";");
        _jdbcTemplate.execute("DROP TABLE " + ScheduledTaskSqlRepository.SCHEDULE_RUN_TABLE + ";");
        _jdbcTemplate.execute("DROP TABLE " + ScheduledTaskSqlRepository.SCHEDULE_TASK_TABLE + ";");
        _jdbcTemplate.execute("DROP TABLE " + MasterLockSqlRepository.MASTER_LOCK_TABLE + ";");
        _jdbcTemplate.execute("DROP TABLE stb_schedule_table_version;");
    }

    @Test
    public void testStartupAndShutdownWithoutAnyScheduledTasks() {
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
            // Register test configuration for scheduled tasks.
            context.getBeanFactory().registerSingleton("dataSource", _dataSource);
            context.register(ScheduledTaskSpringTestConfiguration.class);

            // Refresh context.
            context.refresh();

            // At this point we should have a ScheduledTaskService.
            ScheduledTaskRegistry scheduledTaskRegistry = context.getBean(ScheduledTaskRegistry.class);
            assertNotNull(scheduledTaskRegistry);
        }
    }

    @Test
    public void testAnnotatedSpringComponent() {
        try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext()) {
            // Register test configuration for scheduled tasks.
            context.getBeanFactory().registerSingleton("dataSource", _dataSource);
            context.register(ScheduledTaskSpringTestConfiguration.class);

            // Register a spring bean with an annotated method.
            context.register(SpringComponentWithScheduledTask.class);

            // Refresh context
            context.refresh();

            // At this point we should have a ScheduledTaskService with a registered scheduled task
            ScheduledTaskRegistry scheduledTaskRegistry = context.getBean(ScheduledTaskRegistry.class);
            assertNotNull(scheduledTaskRegistry.getScheduledTask(SpringComponentWithScheduledTask.SCHEDULED_TASK_NAME));
        }
    }

    @Configuration
    @EnableScheduledTasks
    public static class ScheduledTaskSpringTestConfiguration {

    }


    static final String MASTER_TABLE_CREATE_SQL = "CREATE TABLE " + MasterLockSqlRepository.MASTER_LOCK_TABLE + " ( "
            + " lock_name VARCHAR(255) NOT NULL, "
            + " node_name VARCHAR(255) NOT NULL, "
            + " lock_taken_time datetime2 NOT NULL, "
            + " lock_last_updated_time datetime2 NOT NULL, "
            + " CONSTRAINT PK_lock_name PRIMARY KEY (lock_name) "
            + " );";

    static final String SCHEDULE_TABLE_VERSION_CREATE_SQL = "CREATE TABLE stb_schedule_table_version ( "
            + " version int NOT NULL"
            + " ) ";

    static final String STOREBRAND_SCHEDULE_CREATE_SQL =
            "CREATE TABLE " + ScheduledTaskSqlRepository.SCHEDULE_TASK_TABLE + " ( "
                    + " schedule_name VARCHAR(255) NOT NULL, "
                    + " is_active BIT NOT NULL, "
                    + " run_once BIT NOT NULL, "
                    + " cron_expression VARCHAR(255) NULL, "
                    + " next_run DATETIME2 NOT NULL, "
                    + " last_updated DATETIME2 NOT NULL, "
                    + " CONSTRAINT PK_schedule_name PRIMARY KEY (schedule_name) "
                    + " );";

    static final String SCHEDULE_RUN_CREATE_SQL =
            "CREATE TABLE " + ScheduledTaskSqlRepository.SCHEDULE_RUN_TABLE + " ( "
                    + " run_id BIGINT NOT NULL IDENTITY(1, 1), "
                    + " schedule_name VARCHAR(255) NOT NULL, "
                    + " hostname VARCHAR(255) NOT NULL, "
                    + " run_start DATETIME2 NOT NULL, "
                    + " status VARCHAR(250) NULL, "
                    + " status_msg VARCHAR(MAX) NULL, "
                    + " status_stacktrace VARCHAR(MAX) NULL, "
                    + " status_time DATETIME2 NOT NULL, "
                    + " CONSTRAINT PK_run_id PRIMARY KEY (run_id) "
                    + " );";

    static final String SCHEDULE_RUN_INDEX_CREATE_SQL = "CREATE INDEX IX_stb_schedule_run_name_start_status"
            + " ON stb_schedule_run (schedule_name, run_start DESC, status);";

    static final String SCHEDULE_LOG_ENTRY_CREATE_SQL =
            "CREATE TABLE " + ScheduledTaskSqlRepository.SCHEDULE_LOG_ENTRY_TABLE + " ( "
                    + " log_id BIGINT NOT NULL IDENTITY(1, 1),"
                    + " run_id BIGINT NOT NULL, "
                    + " log_msg VARCHAR(MAX) NOT NULL, "
                    + " log_stacktrace VARCHAR(MAX) NULL, "
                    + " log_time DATETIME2 NOT NULL, "
                    + " CONSTRAINT PK_log_id PRIMARY KEY (log_id), "
                    + " CONSTRAINT FK_run_id FOREIGN KEY (run_id) REFERENCES stb_schedule_run (run_id) "
                    + " );";
}
