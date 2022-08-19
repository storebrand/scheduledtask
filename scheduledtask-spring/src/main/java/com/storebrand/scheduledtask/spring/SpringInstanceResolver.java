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

import java.util.Collection;
import java.util.Map;

import org.springframework.context.ApplicationContext;

import com.storebrand.scheduledtask.annotation.ScheduledTask;
import com.storebrand.scheduledtask.annotation.ScheduledTaskInstanceResolver;

/**
 * Instance resolver for {@link ScheduledTask} annotated methods that uses the Spring application context to look for
 * bean instances.
 *
 * @author Kristian Hiim
 */
public class SpringInstanceResolver implements ScheduledTaskInstanceResolver {
    private final ApplicationContext _applicationContext;

    public SpringInstanceResolver(ApplicationContext applicationContext) {
        _applicationContext = applicationContext;
    }

    @Override
    public <T> Collection<T> getInstancesFor(Class<T> clazz) {
        Map<String, T> beans = _applicationContext.getBeansOfType(clazz, false, true);
        return beans.values();
    }
}
