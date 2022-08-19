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

package com.storebrand.scheduledtask.spring;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test mode is a special mode that can be used in unit tests and similar situations where we don't want the background
 * threads to run, but we want to be able to register scheduled tasks, and trigger running them manually.
 * <p>
 * There are two ways to enable test mode. Either by explicitly calling {@link #forceTestMode()}, before creating a
 * {@link ScheduledTaskRegistryFactory}, or by including com.storebrand.scheduledtask:scheduledtask-testing on the class
 * path.
 *
 * @author Kristian Hiim
 */
public final class TestModeUtil {
    private static final Logger log = LoggerFactory.getLogger(TestModeUtil.class);

    private static volatile boolean __forceTestMode = false;
    private static final boolean __testRunnerOnClassPath;
    static {
        Class<?> clazz = null;
        try {
            clazz = Class.forName("com.storebrand.scheduledtask.testing.ScheduledTaskTestRunner");
            log.info("## TEST MODE DETECTED ## - Reason: Found class [" + clazz.getName() + "] on class path.");
        }
        catch (ClassNotFoundException e) {
            // Ignore - we are not in test mode.
        }
        __testRunnerOnClassPath = clazz != null;
    }

    private TestModeUtil() {
        // Hide constructor in utility class
    }

    /**
     * Utility method that tells us if we are running in test mode.
     */
    public static boolean isTestMode() {
        return __forceTestMode || __testRunnerOnClassPath;
    }

    /**
     * Call this before adding the factory to the Spring context, in order to force test mode.
     */
    public static void forceTestMode() {
        log.info("## TEST MODE DETECTED ## - Reason: Forced test mode by calling forceTestMode()");
        __forceTestMode = true;
    }
}
