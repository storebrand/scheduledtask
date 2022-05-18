package com.storebrand.scheduledtask.testing;

import com.storebrand.scheduledtask.ScheduledTask;
import com.storebrand.scheduledtask.ScheduledTaskRegistry.ScheduleRunContext;

public final class ScheduledTaskTestRunner {

    private ScheduledTaskTestRunner() {
        // Utility class
    }

    public static ScheduleRunContext runScheduledTask(ScheduledTask scheduledTask) {
        scheduledTask.runNow();
        return scheduledTask.getLastScheduleRun().orElseThrow();
    }

}
