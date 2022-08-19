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

package com.storebrand.scheduledtask.spring.testscheduledtasks1;

import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.stereotype.Component;

import com.storebrand.scheduledtask.ScheduledTaskRegistry.ScheduleRunContext;
import com.storebrand.scheduledtask.ScheduledTaskRegistry.ScheduleStatus;
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
