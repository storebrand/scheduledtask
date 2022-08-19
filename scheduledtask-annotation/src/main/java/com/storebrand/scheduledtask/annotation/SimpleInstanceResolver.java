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
import java.util.Collection;
import java.util.Collections;

/**
 * Simple instance resolver that will simply try to new up an instance for the given class. This assumes that there is a
 * constructor that requires no arguments.
 *
 * @author Kristian Hiim
 */
public class SimpleInstanceResolver implements ScheduledTaskInstanceResolver {
    @Override
    public <T> Collection<T> getInstancesFor(Class<T> clazz) {
        try {
            return Collections.singleton(clazz.getDeclaredConstructor().newInstance());
        }
        catch (IllegalAccessException | InstantiationException | NoSuchMethodException | InvocationTargetException exception) {
            throw new IllegalStateException("Attempt to new up class [" + clazz + "]", exception);
        }
    }
}
