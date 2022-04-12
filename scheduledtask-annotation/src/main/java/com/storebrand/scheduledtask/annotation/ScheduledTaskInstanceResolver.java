package com.storebrand.scheduledtask.annotation;

import java.util.Collection;

/**
 * Interface for resolving one or more instances of a class with a method annotated with @ScheduledTask.
 *
 * @author Kristian Hiim
 */
public interface ScheduledTaskInstanceResolver {
    /**
     * Returns all instances for a given class. Typically, this only contains one instance, but containers such as
     * Spring might contain multiple beans that implement the same interface.
     */
    <T> Collection<T> getInstancesFor(Class<T> clazz);
}
