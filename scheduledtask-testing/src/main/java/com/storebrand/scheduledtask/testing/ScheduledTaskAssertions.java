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

import java.util.function.Function;

import com.storebrand.scheduledtask.ScheduledTask;
import com.storebrand.scheduledtask.ScheduledTaskRegistry.ScheduleRunContext;
import com.storebrand.scheduledtask.ScheduledTaskRegistry.State;

/**
 * Assertions for checking the status and status message of a {@link ScheduleRunContext} that has been returned from
 * either {@link ScheduledTaskTestRunner#runScheduledTask(ScheduledTask)} or
 * {@link ScheduledTaskTestRunner#runScheduledTaskMethod(Function)}.
 *
 * @author Kristian Hiim
 */
public final class ScheduledTaskAssertions {

    private ScheduledTaskAssertions() {
        // Utility test helper
    }

    public static AssertScheduleRunContext assertThat(ScheduleRunContext context) {
        return new AssertScheduleRunContext(context);
    }

    public static class AssertScheduleRunContext {
        private final ScheduleRunContext _context;

        private AssertScheduleRunContext(ScheduleRunContext context) {
            _context = context;
        }

        /**
         * Assert that the scheduled task has a specific state.
         */
        public AssertScheduleRunContext hasStatus(State expectedState) {
            if (_context.getStatus() != expectedState) {
                throw new AssertionError("Expected Scheduled run status [" + expectedState.name()
                        + "], but status was [" +  _context.getStatus() + "]");
            }
            return this;
        }

        /**
         * Assert that the scheduled task was completed by calling {@link ScheduleRunContext#done(String)}.
         */
        public AssertScheduleRunContext isDone() {
            return hasStatus(State.DONE);
        }

        /**
         * Assert that the scheduled task was completed by calling {@link ScheduleRunContext#failed(String)} or the
         * alternative with throwable {@link ScheduleRunContext#failed(String, Throwable)}.
         */
        public AssertScheduleRunContext hasFailed() {
            return hasStatus(State.FAILED);
        }

        /**
         * Assert that the scheduled task method returned by calling {@link ScheduleRunContext#dispatched(String)}.
         */
        public AssertScheduleRunContext isDispatched() {
            return hasStatus(State.DISPATCHED);
        }

        /**
         * Assert that the scheduled task has a specific expected status message.
         */
        public AssertScheduleRunContext hasStatusMessage(String expectedMessage) {
            if (expectedMessage == null) {
                if (_context.getStatus() == null) {
                    return this;
                }
                throw new AssertionError("Expected no status message, but status message was ["
                        + _context.getStatusMsg() + "]");
            }

            if (!expectedMessage.equals(_context.getStatusMsg())) {
                throw new AssertionError("Expected status message [" + expectedMessage
                        + "], but status message was [" + _context.getStatusMsg() + "]");
            }
            return this;
        }

    }

}
