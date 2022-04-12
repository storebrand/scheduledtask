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
