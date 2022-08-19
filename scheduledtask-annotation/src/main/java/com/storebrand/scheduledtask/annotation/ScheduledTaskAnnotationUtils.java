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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.storebrand.scheduledtask.ScheduledTaskRegistry;
import com.storebrand.scheduledtask.ScheduledTaskRegistry.ScheduleRunContext;
import com.storebrand.scheduledtask.ScheduledTaskRegistry.ScheduleStatus;

/**
 * Utility class for interacting with {@link ScheduledTask} annotated methods.
 *
 * @author Kristian Hiim
 */
public class ScheduledTaskAnnotationUtils {
    private static final Logger log = LoggerFactory.getLogger(ScheduledTaskAnnotationUtils.class);

    /**
     * Validates if a method is of format "ScheduleStatus methodName(ScheduleRunContext)".
     *
     * @param method
     *         the scheduled task method we should validate.
     * @return true if this is a valid scheduled task method.
     */
    public static boolean isValidScheduledTaskMethod(Method method) {
        // ?: Does this method return ScheduleStatus?
        if (method.getReturnType() != ScheduleStatus.class) {
            // -> Nope, then this is not a valid method for a Scheduled Task
            return false;
        }

        // Chec kif there is exactly one argument of type ScheduleRunContext
        Class<?>[] parameterTypes = method.getParameterTypes();
        return parameterTypes.length == 1
                && parameterTypes[0] == ScheduleRunContext.class;
    }

    /**
     * Register a method annotated with {@link ScheduledTask} annotation as a scheduled task.
     *
     * @param method
     *         the method with {@link ScheduledTask} annotation.
     * @param scheduledTaskRegistry
     *         the scheduled task service, where it should be registered.
     * @param instanceResolver
     *         an instance resolver that will fetch us an instance that will run the method.
     */
    public static void registerMethod(Method method, ScheduledTaskRegistry scheduledTaskRegistry,
            ScheduledTaskInstanceResolver instanceResolver) {
        if (!method.isAnnotationPresent(ScheduledTask.class)) {
            throw new IllegalArgumentException("Annotation @ScheduledTask not present on method.");
        }
        if (!isValidScheduledTaskMethod(method)) {
            throw new IllegalArgumentException(
                    "Method must return ScheduleStatus, and have exactly one argument of type ScheduleRunContext.");
        }
        ScheduledTask annotation = method.getAnnotation(ScheduledTask.class);

        Collection<?> instances = instanceResolver.getInstancesFor(method.getDeclaringClass());

        if (instances.isEmpty()) {
            throw new IllegalStateException("Unable to resolve instances for class "
                    + method.getDeclaringClass().getName());
        }

        int number = 1;

        for (Object instance : instances) {
            String name = instances.size() == 1
                    ? annotation.name()
                    : annotation.name() + "#" + number++;

            log.info("Registering scheduled task annotated method [" + name + "]");
            scheduledTaskRegistry.buildScheduledTask(name, annotation.cronExpression(), context -> {
                try {
                    return (ScheduleStatus) method.invoke(instance, context);
                }
                catch (IllegalAccessException | InvocationTargetException e) {
                    throw new RuntimeException("Unable to invoke method " + method.getName() + " on class "
                            + method.getDeclaringClass().getName());
                }
            })
                    .maxExpectedMinutesToRun(annotation.maxExpectedMinutesToRun())
                    .deleteRunsAfterDays(annotation.deleteRunsAfterDays())
                    .deleteSuccessfulRunsAfterDays(annotation.deleteSuccessfulRunsAfterDays())
                    .deleteFailedRunsAfterDays(annotation.deleteFailedRunsAfterDays())
                    .keepMaxRuns(annotation.keepMaxRuns())
                    .keepMaxSuccessfulRuns(annotation.keepMaxSuccessfulRuns())
                    .keepMaxFailedRuns(annotation.keepMaxFailedRuns())
                    .start();
        }
    }
}