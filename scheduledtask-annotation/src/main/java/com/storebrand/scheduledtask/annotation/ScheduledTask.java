package com.storebrand.scheduledtask.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import com.storebrand.scheduledtask.ScheduledTask.Criticality;
import com.storebrand.scheduledtask.ScheduledTask.Recovery;
import com.storebrand.scheduledtask.ScheduledTaskInitializer;

/**
 * Annotation that can be used to create a scheduled task for a method. Apply to any method and the task will run
 * according to the specified {@link #cronExpression()}.
 * <p>
 * It is possible to further specify details about the task by overriding the default values below. Look at
 * documentation for each setting to see what it does.
 *
 * @author Kristian Hiim
 */
// Allow scanning for this annotation on runtime
@Retention(RetentionPolicy.RUNTIME)
// Use in method only
@Target(ElementType.METHOD)
public @interface ScheduledTask {
    /**
     * The name of this scheduled task. All tasks must have a unique name.
     */
    String name();

    /**
     * A cron expression for when this scheduled task should run.
     */
    String cronExpression();

    /**
     * The maximum time this task is expected to run, in minutes. If it takes longer than this then something might be
     * wrong, and we should investigate the scheduled task.
     */
    int maxExpectedMinutesToRun() default ScheduledTaskInitializer.DEFAULT_MAX_EXPECTED_MINUTES_TO_RUN;

    /**
     * The {@link Criticality} of this scheduled task. Determines how important this task is, and can be used by
     * monitoring systems to show different error messages based on criticality level.
     */
    Criticality criticality() default Criticality.IMPORTANT;

    /**
     * Is this scheduled task self-healing, or does failures in this task require manual intervention?
     * <p>
     * The default assumption is that a scheduled task should be able to recover by itself when the reason for failures
     * is gone. This could be because an external service was temporarily unavailable, or there was a network issue.
     * When this issue is resolved the next run of a scheduled task will work, and the task will recover or complete
     * anything it was not able to do during failed runs.
     * <p>
     * Even if a scheduled task is set to self-healing it is still a good idea to monitor it to make sure it gets up and
     * running again. And any external issues might still require some form of intervention to fix.
     * <p>
     * If a failed task will not be able to recover by itself it should be set to {@link Recovery#MANUAL_INTERVENTION}.
     * In this case a failed task must be handled by a human, and we should not expect it to simply recover by itself by
     * running it again.
     */
    Recovery recovery() default Recovery.SELF_HEALING;

    /**
     * Define the number of days we should keep a record of runs for this schedule. After this records will be deleted.
     * The default is to delete records after 365 days. Set to 0 to disable this rule.
     */
    int deleteRunsAfterDays() default ScheduledTaskInitializer.DEFAULT_DELETE_RUNS_AFTER_DAYS;

    /**
     * Define the number of days we should keep a record of successful runs for this schedule. This is not enabled by
     * default. If both this and {@link #deleteRunsAfterDays()} is used then both rules will be applied, and the lowest
     * of the two will be used to determine when to delete successful runs.
     */
    int deleteSuccessfulRunsAfterDays() default 0;

    /**
     * Define the number of days we should keep a record of failed runs for this schedule. This is not enabled by
     * default. If both this and {@link #deleteRunsAfterDays()} is used then both rules will be applied, and the lowest
     * of the two will be used to determine when to delete failed runs.
     */
    int deleteFailedRunsAfterDays() default 0;

    /**
     * Only keep this many runs. Older records will be deleted if there are more. This rule is disabled by default.
     */
    int keepMaxRuns() default 0;

    /**
     * Only keep this many successful runs. Older records will be deleted if there are more. This rule is disabled by
     * default. If both this and {@link #keepMaxRuns()} is used then both will be applied, and the lowest number will be
     * used to determine how many to keep.
     */
    int keepMaxSuccessfulRuns() default 0;

    /**
     * Only keep this many failed runs. Older records will be deleted if there are more. This rule is disabled by
     * default. If both this and {@link #keepMaxRuns()} is used then both will be applied, and the lowest number will be
     * used to determine how many to keep.
     */
    int keepMaxFailedRuns() default 0;
}
