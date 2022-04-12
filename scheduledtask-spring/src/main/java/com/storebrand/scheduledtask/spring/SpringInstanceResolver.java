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
