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

package com.storebrand.scheduledtask;

import com.storebrand.scheduledtask.ScheduledTask.Criticality;
import com.storebrand.scheduledtask.ScheduledTask.Recovery;
import com.storebrand.scheduledtask.ScheduledTaskRegistry.ScheduleRunnable;

/**
 * Initializer that is used to build a scheduled task. Get an implementation by calling
 * {@link ScheduledTaskRegistry#buildScheduledTask(String, String, ScheduleRunnable)}, and initialize the scheduled task by
 * filling in properties, and finally calling {@link ScheduledTaskBuilder#start()}.
 *
 * @author Kristian Hiim
 */
public interface ScheduledTaskBuilder {

    int DEFAULT_MAX_EXPECTED_MINUTES_TO_RUN = 5;
    Criticality DEFAULT_CRITICALITY = Criticality.IMPORTANT;
    Recovery DEFAULT_RECOVERY = Recovery.SELF_HEALING;
    int DEFAULT_DELETE_RUNS_AFTER_DAYS = 365;
    int DEFAULT_DELETE_NOOP_RUNS_AFTER_DAYS = 7;
    int DEFAULT_KEEP_MAX_NOOP_RUNS = 100;

    /**
     * Define the maximum minutes this task is expected to run.
     *
     * @param minutes
     *         the maximum minutes this task should run.
     * @return the initializer that builds the {@link ScheduledTask}.
     */
    ScheduledTaskBuilder maxExpectedMinutesToRun(int minutes);

    /**
     * Define how critical this task is. Default is {@link Criticality#IMPORTANT}.
     *
     * @param criticality
     *         the criticality level of this scheduled task.
     * @return the initializer that builds the {@link ScheduledTask}.
     */
    ScheduledTaskBuilder criticality(Criticality criticality);

    /**
     * Defines if this task is able to recover by itself or not. Default is {@link Recovery#SELF_HEALING}.
     *
     * @param recovery
     *         the task is either self-healing or requires manual intervention if it fails.
     * @return the initializer that builds the {@link ScheduledTask}.
     */
    ScheduledTaskBuilder recovery(Recovery recovery);

    /**
     * Define the number of days we should keep a record of runs for this schedule. After this records will be deleted.
     * The default is to delete records after 365 days. Set to 0 to disable this rule.
     *
     * @param days
     *         after this number of days records of scheduled runs will be deleted.
     * @return the initializer that builds the {@link ScheduledTask}.
     */
    ScheduledTaskBuilder deleteRunsAfterDays(int days);

    /**
     * Define the number of days we should keep a record of successful runs for this schedule. This is not enabled by
     * default. If both this and {@link #deleteRunsAfterDays(int)} is used then both rules will be applied, and the
     * lowest of the two will be used to determine when to delete successful runs.
     *
     * @param days
     *         after this number of days we should delete records of successful runs.
     * @return the initializer that builds the {@link ScheduledTask}.
     */
    ScheduledTaskBuilder deleteSuccessfulRunsAfterDays(int days);

    /**
     * Define the number of days we should keep a record of failed runs for this schedule. This is not enabled by
     * default. If both this and {@link #deleteRunsAfterDays(int)} is used then both rules will be applied, and the
     * lowest of the two will be used to determine when to delete failed runs.
     *
     * @param days
     *         after this number of days we should delete records of failed runs.
     * @return the initializer that builds the {@link ScheduledTask}.
     */
    ScheduledTaskBuilder deleteFailedRunsAfterDays(int days);

    /**
     * Define the number of days we should keep a record of noop runs for this schedule. By default this is 1 week.
     * If both this and {@link #deleteRunsAfterDays(int)} is used then both rules will be applied, and the
     * lowest of the two will be used to determine when to delete failed runs.
     *
     * @param days
     *         after this number of days we should delete records of noop runs.
     * @return the initializer that builds the {@link ScheduledTask}.
     */
    ScheduledTaskBuilder deleteNoopRunsAfterDays(int days);

    /**
     * Only keep this many runs. Older records will be deleted if there are more. This rule is disabled by default.
     *
     * @param maxRuns
     *         the maximum number of runs we should keep records of.
     * @return the initializer that builds the {@link ScheduledTask}.
     */
    ScheduledTaskBuilder keepMaxRuns(int maxRuns);

    /**
     * Only keep this many successful runs. Older records will be deleted if there are more. This rule is disabled by
     * default. If both this and {@link #keepMaxRuns(int)} is used then both will be applied, and the lowest number will
     * be used to determine how many to keep.
     *
     * @param maxSuccessfulRuns
     *         the maximum number of successful runs we should keep records of.
     * @return the initializer that builds the {@link ScheduledTask}.
     */
    ScheduledTaskBuilder keepMaxSuccessfulRuns(int maxSuccessfulRuns);

    /**
     * Only keep this many failed runs. Older records will be deleted if there are more. This rule is disabled by
     * default. If both this and {@link #keepMaxRuns(int)} is used then both will be applied, and the lowest number will
     * be used to determine how many to keep.
     *
     * @param maxFailedRuns
     *         the maximum number of failed runs we should keep records of.
     * @return the initializer that builds the {@link ScheduledTask}.
     */
    ScheduledTaskBuilder keepMaxFailedRuns(int maxFailedRuns);


    /**
     * Only keep this many noop runs. Older records will be deleted if there are more. This rule is set to 100 records
     * by default. If both this and {@link #keepMaxRuns(int)} is used then both will be applied, and the lowest number
     * will be used to determine how many to keep.
     *
     * @param maxNoopRuns
     *         the maximum number of noop runs we should keep records of.
     * @return the initializer that builds the {@link ScheduledTask}.
     */
    ScheduledTaskBuilder keepMaxNoopRuns(int maxNoopRuns);

    /**
     * Initializes and starts the scheduled task. It is important to call this, or the scheduled task will not be
     * created.
     *
     * @return the initialized scheduled task.
     * @throws DuplicateScheduledTaskException
     *         if the name is not unique.
     */
    ScheduledTask start();


    /**
     * Exception thrown if one tries to create a scheduled task with a name that already exists.
     */
    class DuplicateScheduledTaskException extends RuntimeException {
        DuplicateScheduledTaskException(String name) {
            super("A scheduled task with the name [" + name + "] already exists.");
        }
    }
}
