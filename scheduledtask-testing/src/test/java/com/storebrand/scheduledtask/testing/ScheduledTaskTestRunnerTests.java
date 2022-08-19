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
