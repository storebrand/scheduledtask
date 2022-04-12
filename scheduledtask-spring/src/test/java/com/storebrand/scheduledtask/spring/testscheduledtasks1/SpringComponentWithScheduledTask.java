package com.storebrand.scheduledtask.spring.testscheduledtasks1;

import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.stereotype.Component;

import com.storebrand.scheduledtask.ScheduledTaskService.ScheduleRunContext;
import com.storebrand.scheduledtask.ScheduledTaskService.ScheduleStatus;
import com.storebrand.scheduledtask.annotation.ScheduledTask;

@Component
public class SpringComponentWithScheduledTask {

    public static final String SCHEDULED_TASK_NAME = "annotated-method-in-spring-bean";

    private final AtomicInteger _counter = new AtomicInteger();

    @ScheduledTask(name = SCHEDULED_TASK_NAME, cronExpression = "0 * * * * *")
    public ScheduleStatus runTask(ScheduleRunContext context) {
        _counter.incrementAndGet();
        return context.done("Done");
    }

    public int getCounter() {
        return _counter.get();
    }
}
