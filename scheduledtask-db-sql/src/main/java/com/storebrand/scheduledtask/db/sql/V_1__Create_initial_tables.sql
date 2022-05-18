--------------------------------------------------
-- stb_master_locker
--------------------------------------------------
-- Table for keeping track of the ScheduledTask master locks
-- Column lock_name: the name of the scheduler
-- Column node_name: node of the server that currently has the lock
-- Column lock_taken_time: when this lock was last taken
-- Column lock_last_updated_time: used by the master node to keep the lock
--
-- The columns lock_taken_time and lock_last_updated_time is used to keep track on the master node
-- to see if he currently is actively keeping the lock and how long he has kept it.
CREATE TABLE stb_schedule_master_locker (
    lock_name VARCHAR(255) NOT NULL,
    node_name VARCHAR(255) NOT NULL,
    lock_taken_time datetime2 NOT NULL,
    lock_last_updated_time datetime2 NOT NULL,
    CONSTRAINT PK_lock_name PRIMARY KEY (lock_name)
);

--------------------------------------------------
-- stb_schedule_table_version
--------------------------------------------------
-- This table holds a counter on what version this file 'Create_initial_tables' are. It is used during startup of the
-- scheduler to verify we have the same version in the service as goodies expects.
CREATE TABLE stb_schedule_table_version (
    version int NOT NULL
);

-- If changes are done to these tables this value should increase by one.
INSERT INTO stb_schedule_table_version (version) VALUES (1);

--------------------------------------------------
-- stb_schedule
--------------------------------------------------
-- Table for keeping track of the ScheduledTasks
-- Column schedule_name: the name of the schedule
-- Column is_active: flag that informs if this schedule is active (IE is running or paused)
-- Column run_once: flag that informs that this schedule should run immediately regardless of next_run
-- Column cron_expression: When null the default coded in the java file will be used. if set then tis is the override
-- Column next_run: timestamp on when the schedule should be running next time
-- Column last_updated: Timestamp when this row was last updated. IE when the last run was triggered.
--
-- Note the last_updated may be set even if the is_active is false. This means the execution of the schedule is
-- deactivated but it will be doing it's normal schedule "looping"
CREATE TABLE stb_schedule (
    schedule_name VARCHAR(255) NOT NULL,
    is_active BIT NOT NULL,
    run_once BIT NOT NULL,
    cron_expression VARCHAR(255) NULL,
    next_run datetime2 NOT NULL,
    last_updated datetime2 NOT NULL,
    CONSTRAINT PK_schedule_name PRIMARY KEY (schedule_name)
);

--------------------------------------------------
-- stb_schedule_run
--------------------------------------------------
-- Table for scheduleRun. This has the run history the schedules
-- Column run_id: ID for the run.
-- Column schedule_name: the name of the schedule
-- Column hostname: host that this instance runs on.
-- Column run_start: When this schedule was started.
-- Column status state of the schedule run, should be one of ScheduleTaskImpl.State STARTED/FAILED/DISPATCHED/DONE
-- Column status_msg: Some informing text that is connected to the state.
-- Column status_stacktrace: Can only be set on STATUS = FAILED and can contain a stacktrace
-- Column status_time: When this schedule state was set.
CREATE TABLE stb_schedule_run (
    run_id BIGINT NOT NULL IDENTITY(1, 1),
    schedule_name VARCHAR(255) NOT NULL,
    hostname VARCHAR(255) NOT NULL,
    run_start DATETIME2 NOT NULL,
    status VARCHAR(250) NULL,
    status_msg VARCHAR(MAX) NULL,
    status_stacktrace VARCHAR(MAX) NULL,
    status_time DATETIME2 NOT NULL,
    CONSTRAINT PK_run_id PRIMARY KEY (run_id)
);

CREATE INDEX IX_stb_schedule_run_name_start_status ON stb_schedule_run (schedule_name, run_start DESC, status);

--------------------------------------------------
-- stb_schedule_log_entry
--------------------------------------------------
-- Table for scheduleRunLogs storing the logs (if any) for a scheduleRun
-- Column: log_id: primary key id for each log line.
-- Column: run_id: ID for the run. Foreign Key is from the scheduleRun table.
-- Column: log_msg: message that is logged for the run.
-- Column: log_stacktrace: If set contains a stacktrace in addition to the log_msg.
-- Column: log_time: timestamp on when this log was recorded.
CREATE TABLE stb_schedule_log_entry (
    log_id BIGINT NOT NULL IDENTITY(1, 1),
    run_id BIGINT NOT NULL,
    log_msg VARCHAR(MAX) NOT NULL,
    log_stacktrace VARCHAR(MAX) NULL,
    log_time DATETIME2 NOT NULL,
    CONSTRAINT PK_log_id PRIMARY KEY (log_id),
    CONSTRAINT FK_run_id FOREIGN KEY (run_id) REFERENCES stb_schedule_run (run_id)
);
