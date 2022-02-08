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
    lock_name VARCHAR NOT NULL,
    node_name VARCHAR NOT NULL,
    lock_taken_time datetime2 NOT NULL,
    lock_last_updated_time datetime2 NOT NULL,
    CONSTRAINT PK_lock_name PRIMARY KEY (lock_name)
);

--------------------------------------------------
-- stb_schedule_table_version
--------------------------------------------------
-- This table holds a counter on what version this file 'Create_initial_tables' are. It is used during starup of the
-- scheduler to verify we have the same version in the service as goodies expects.
CREATE TABLE stb_schedule_table_version (
    version int NOT NULL
);

-- If changes are done to these tables this value should increase by one.
INSERT INTO stb_schedule_table_version (version) VALUES (1);

--------------------------------------------------
-- stb_schedule
--------------------------------------------------
-- Table for keeping track of the ScheduledTask's
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
    schedule_name VARCHAR NOT NULL,
    is_active BIT NOT NULL,
    run_once BIT NOT NULL,
    cron_expression VARCHAR NULL,
    next_run datetime2 NOT NULL,
    last_updated datetime2 NOT NULL,
    CONSTRAINT PK_schedule_name PRIMARY KEY (schedule_name)
);

--------------------------------------------------
-- stb_schedule_run
--------------------------------------------------
-- Table for scheduleRun. This has the run history the schedules
-- Column schedule_name: the name of the schedule
-- Column instance_id: Instance for the run. = runTime + "-" + nodeName
-- Column run_start: When this schedule was started.
-- Column status state of the schedule run, should be one of ScheduleTaskImpl.State STARTªED/FAILED/DISPATCHED/DONE
-- Column status_msg: Some informing text that is connected to the state.
-- Column status_throwable: Can only be set on STATUS = FAILED and can contain a throwable
-- Column status_time: When this schedule state was set.
CREATE TABLE stb_schedule_run (
    instance_id VARCHAR NOT NULL,
    schedule_name VARCHAR NOT NULL,
    run_start DATETIME2 NOT NULL,
    status VARCHAR NULL,
    status_msg VARCHAR NULL,
    status_throwable VARCHAR NULL,
    status_time DATETIME2 NOT NULL,
    CONSTRAINT PK_instance_id PRIMARY KEY (instance_id)
);

--------------------------------------------------
-- stb_schedule_log_entry
--------------------------------------------------
-- Table for scheduleRunLogs storing the logs (if any) for a scheduleRun
-- Column: instance_id: Instance for the run. = runTime + "-" + nodeName. Foreign Key is from the scheduleRun table
-- Column: log_msg: message that is logged for the run.
-- Column: log_throwable: If set contains a throwable in addition to the log_msg
-- Column: log_time: timestamp on when this log was recorded.
CREATE TABLE stb_schedule_log_entry (
    instance_id VARCHAR NOT NULL,
    log_msg VARCHAR NOT NULL,
    log_throwable VARCHAR NULL,
    log_time datetime2 NOT NULL,
    CONSTRAINT FK_instance_id FOREIGN KEY (instance_id) REFERENCES stb_schedule_run (instance_id)
);
