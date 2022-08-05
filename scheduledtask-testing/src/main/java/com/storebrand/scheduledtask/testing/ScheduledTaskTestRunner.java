package com.storebrand.scheduledtask.testing;

import static com.storebrand.scheduledtask.testing.MockRunContext.SCHEDULE_STATUS_VALID_RESPONSE_CLASS;

import java.util.function.Function;

import com.storebrand.scheduledtask.ScheduledTask;
import com.storebrand.scheduledtask.ScheduledTaskRegistry;
import com.storebrand.scheduledtask.ScheduledTaskRegistry.ScheduleRunContext;
import com.storebrand.scheduledtask.ScheduledTaskRegistry.ScheduleStatus;

/**
 * Helper class for running scheduled tasks in unit tests.
 *
 * @author Kristian Hiim
 */
public final class ScheduledTaskTestRunner {

    private ScheduledTaskTestRunner() {
        // Utility class
    }

    /**
     * Run a {@link ScheduledTask} and return the {@link ScheduleRunContext} so it can be inspected. It is possible to
     * use {@link ScheduledTaskAssertions} to inspect the {@link ScheduleRunContext}. This is meant to be used in
     * integration tests, where you have a fully wired {@link ScheduledTaskRegistry} available.
     *
     * @param scheduledTask
     *         a {@link ScheduledTask} that has been created from a {@link ScheduledTaskRegistry}.
     * @return the {@link ScheduleRunContext} so it can be inspected.
     */
    public static ScheduleRunContext runScheduledTask(ScheduledTask scheduledTask) {
        scheduledTask.runNow();
        return scheduledTask.getLastScheduleRun().orElseThrow();
    }

    /**
     * Directly runs a scheduled task method, without going through a {@link ScheduledTaskRegistry}. This is meant to be
     * used by unit tests that tests the specific method, and you should use mocks for verifying the behavior of the
     * method.
     * <p>
     * The method will be supplied with a mock {@link ScheduleRunContext}, that just keeps state in-memory. This can be
     * used with {@link ScheduledTaskAssertions} to assert the status of the scheduled task after running the method.
     *
     * @param method
     *         a scheduled task method.
     * @return a mock {@link ScheduleRunContext} that can be inspected.
     */
    public static ScheduleRunContext runScheduledTaskMethod(Function<ScheduleRunContext, ScheduleStatus> method) {
        MockRunContext context = new MockRunContext();
        ScheduleStatus status = method.apply(context);
        if (SCHEDULE_STATUS_VALID_RESPONSE_CLASS.equals(status.getClass().getName())) {
            return context;
        }

        throw new AssertionError("Scheduled task method did not return a valid response object of type ["
                + SCHEDULE_STATUS_VALID_RESPONSE_CLASS + "]."
                + " You must call the methods \"done\" or \"failed\" on ScheduleRunContext to return a valid response.");
    }

}
