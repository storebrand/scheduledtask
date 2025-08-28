-- Rename datetime columns to have _utc suffix
ALTER TABLE stb_schedule_master_locker
    RENAME COLUMN lock_taken_time TO lock_taken_time_utc;

ALTER TABLE stb_schedule_master_locker
    RENAME COLUMN lock_last_updated_time TO lock_last_updated_time_utc;

ALTER TABLE stb_schedule
    RENAME COLUMN next_run TO next_run_utc;

ALTER TABLE stb_schedule
    RENAME COLUMN last_updated TO last_updated_utc;

ALTER TABLE stb_schedule_run
    RENAME COLUMN run_start TO run_start_utc;

ALTER TABLE stb_schedule_run
    RENAME COLUMN status_time TO status_time_utc;

ALTER TABLE stb_schedule_log_entry
    RENAME COLUMN log_time TO log_time_utc;

-- Update table version to 2
UPDATE stb_schedule_table_version SET version = 2;

-- Change run_once column from bit to varchar
ALTER TABLE stb_schedule
    ALTER COLUMN run_once VARCHAR(100) NULL;
