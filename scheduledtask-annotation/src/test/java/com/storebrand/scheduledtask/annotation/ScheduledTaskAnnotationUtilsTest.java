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

package com.storebrand.scheduledtask.annotation;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import com.storebrand.scheduledtask.ScheduledTaskRegistry.ScheduleRunContext;
import com.storebrand.scheduledtask.ScheduledTaskRegistry.ScheduleStatus;

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
