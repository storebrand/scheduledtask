package com.storebrand.scheduledtask.annotation;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import com.storebrand.scheduledtask.ScheduledTaskService.ScheduleRunContext;
import com.storebrand.scheduledtask.ScheduledTaskService.ScheduleStatus;

public class ScheduledTaskAnnotationUtilsTest {

    @Test
    public void validateScheduledTaskMethod() throws NoSuchMethodException {
        assertTrue(ScheduledTaskAnnotationUtils.isValidScheduledTaskMethod(
                getClass().getMethod("validScheduledTaskMethod", ScheduleRunContext.class)));
        assertFalse(ScheduledTaskAnnotationUtils.isValidScheduledTaskMethod(
                getClass().getMethod("invalidScheduledTaskMethod1", ScheduleRunContext.class)));
        assertFalse(ScheduledTaskAnnotationUtils.isValidScheduledTaskMethod(
                getClass().getMethod("invalidScheduledTaskMethod2", ScheduleRunContext.class)));
        assertFalse(ScheduledTaskAnnotationUtils.isValidScheduledTaskMethod(
                getClass().getMethod("invalidScheduledTaskMethod3", String.class)));
        assertFalse(ScheduledTaskAnnotationUtils.isValidScheduledTaskMethod(
                getClass().getMethod("invalidScheduledTaskMethod4")));
    }


    // ===== Helper test methods =======================================================================================

    public ScheduleStatus validScheduledTaskMethod(ScheduleRunContext context) {
        return context.done("This is ok");
    }

    public int invalidScheduledTaskMethod1(ScheduleRunContext context) {
        return 0;
    }

    public void invalidScheduledTaskMethod2(ScheduleRunContext context) {
    }

    public ScheduleStatus invalidScheduledTaskMethod3(String wrong) {
        return null;
    }

    public ScheduleStatus invalidScheduledTaskMethod4() {
        return null;
    }

}
