package com.storebrand.scheduledtask.testing;

import org.junit.jupiter.api.Test;

import com.storebrand.scheduledtask.ScheduledTaskRegistry.ScheduleRunContext;
import com.storebrand.scheduledtask.ScheduledTaskRegistry.ScheduleStatus;


/**
 * Tests for {@link ScheduledTaskTestRunner}. Also utilizes {@link ScheduledTaskAssertions}.
 */
class ScheduledTaskTestRunnerTests {

    @Test
    void testScheduledTaskThatCompletesOk() {
        ScheduleRunContext result = ScheduledTaskTestRunner.runScheduledTaskMethod(this::completesOk);
        ScheduledTaskAssertions.assertThat(result)
                .isDone()
                .hasStatusMessage("This is done");
    }

    @Test
    void testScheduledTaskThatDispatches() {
        ScheduleRunContext result = ScheduledTaskTestRunner.runScheduledTaskMethod(this::dispatches);
        ScheduledTaskAssertions.assertThat(result)
                .isDispatched()
                .hasStatusMessage("This is dispatched");
    }

    @Test
    void testScheduledTaskThatFails() {
        ScheduleRunContext result = ScheduledTaskTestRunner.runScheduledTaskMethod(this::fails);
        ScheduledTaskAssertions.assertThat(result)
                .hasFailed()
                .hasStatusMessage("This task failed");
    }

    /**
     * A scheduled task method that completes without errors.
     */
    ScheduleStatus completesOk(ScheduleRunContext context) {
        return context.done("This is done");
    }

    /**
     * A scheduled task method that uses the dispatched status.
     */
    ScheduleStatus dispatches(ScheduleRunContext context) {
        return context.dispatched("This is dispatched");
    }

    /**
     * A scheduled task method that fails.
     */
    ScheduleStatus fails(ScheduleRunContext context) {
        return context.failed("This task failed");
    }
}
