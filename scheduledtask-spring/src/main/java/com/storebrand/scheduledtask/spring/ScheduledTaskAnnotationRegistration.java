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

import static java.util.stream.Collectors.toList;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Role;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.util.ClassUtils;

import com.storebrand.scheduledtask.ScheduledTaskRegistry;
import com.storebrand.scheduledtask.annotation.CombinedInstanceResolver;
import com.storebrand.scheduledtask.annotation.ScheduledTask;
import com.storebrand.scheduledtask.annotation.ScheduledTaskAnnotationUtils;
import com.storebrand.scheduledtask.annotation.ScheduledTaskInstanceResolver;
import com.storebrand.scheduledtask.annotation.SimpleInstanceResolver;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Add this bean to Spring in order to automatically register all {@link ScheduledTask} annotated methods in beans.
 * <p>
 * This requires an implementation of {@link ScheduledTaskRegistry} to be present in the Spring context. The easiest way
 * to get that is to use {@link EnableScheduledTasks}. That will also import this bean.
 * <p>
 * If you want control over instance creation it is possible to add an implementation of
 * {@link ScheduledTaskInstanceResolver} to the Spring context. The default instance resolver will use a
 * {@link CombinedInstanceResolver} that first attempts to resolve using {@link SpringInstanceResolver}, and then moves
 * on to {@link SimpleInstanceResolver} if a bean is not found for the class with the given annotated method.
 *
 * @author Kristian Hiim
 */
@Role(BeanDefinition.ROLE_INFRASTRUCTURE)
public class ScheduledTaskAnnotationRegistration implements BeanPostProcessor, ApplicationContextAware {
    private static final Logger log = LoggerFactory.getLogger(ScheduledTaskAnnotationRegistration.class);
    private static final String LOG_PREFIX = "#SPRINGSCHEDULEDTASK# ";

    private final Set<Class<?>> _classesThatHaveBeenChecked = ConcurrentHashMap.newKeySet();
    private final Set<Method> _methodsWithScheduledTaskAnnotations = ConcurrentHashMap.newKeySet();
    private final Set<Method> _registeredMethodsWithScheduledTaskAnnotations = ConcurrentHashMap.newKeySet();

    private ConfigurableApplicationContext _applicationContext;
    private ConfigurableListableBeanFactory _configurableListableBeanFactory;

    private ScheduledTaskRegistry _scheduledTaskRegistry;
    private ScheduledTaskInstanceResolver _instanceResolver;

    @Override
    @SuppressFBWarnings("EI_EXPOSE_REP2")
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        if (!(applicationContext instanceof ConfigurableApplicationContext)) {
            throw new IllegalStateException("The ApplicationContext when using ScheduledTask SpringConfig"
                    + " must implement " + ConfigurableApplicationContext.class.getSimpleName()
                    + ", while the provided ApplicationContext is of type [" + applicationContext.getClass().getName()
                    + "], and evidently don't.");
        }
        _applicationContext = (ConfigurableApplicationContext) applicationContext;

        // NOTICE!! We CAN NOT touch the _beans_ at this point, since we then will create them, and we will therefore
        // be hit by the "<bean> is not eligible for getting processed by all BeanPostProcessors" - the
        // BeanPostProcessor in question being ourselves!
        // (.. However, the BeanDefinitions is okay to handle.)
        _configurableListableBeanFactory = _applicationContext.getBeanFactory();
    }

    @Override
    public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
        // No need to do anything before beans are initialized
        return bean;
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        // :: Get the BeanDefinition, to check for type.
        BeanDefinition beanDefinition;
        try {
            beanDefinition = _configurableListableBeanFactory.getBeanDefinition(beanName);
        }
        catch (NoSuchBeanDefinitionException e) {
            // -> This is a non-registered bean, which evidently is used when doing unit tests with JUnit SpringRunner.
            log.info(LOG_PREFIX + getClass().getSimpleName()
                    + ".postProcessAfterInitialization(bean, \"" + beanName
                    + "\"): Found no bean definition for the given bean name! Test class?! Ignoring.");
            return bean;
        }

        Class<?> targetClass = ClassUtils.getUserClass(bean);
        // ?: Have we checked this bean before? (might happen with prototype beans)
        if (_classesThatHaveBeenChecked.contains(targetClass)) {
            // -> Yes, we've checked it before, and it either has no @HealthCheck-annotations, or we have already
            // registered the methods with annotations.
            return bean;
        }

        // E-> must check this bean.
        List<Method> methodsWithScheduledTaskAnnotation = Arrays.stream(targetClass.getMethods())
                .filter(method -> AnnotationUtils.findAnnotation(method, ScheduledTask.class) != null)
                .collect(toList());
        // ?: Are there no annotated methods?
        if (methodsWithScheduledTaskAnnotation.isEmpty()) {
            // -> There are no @ScheduledTask annotations, add it to list of checked classes, and return bean
            _classesThatHaveBeenChecked.add(targetClass);
            return bean;
        }

        // Assert that it is a singleton. NOTICE: It may be prototype, but also other scopes like request-scoped.
        if (!beanDefinition.isSingleton()) {
            throw new BeanCreationException("The bean [" + beanName + "] is not a singleton (scope: ["
                    + beanDefinition.getScope() + "]), which does not make sense when it comes to beans that have"
                    + " methods annotated with @ScheduledTask-annotations.");
        }

        for (Method method : methodsWithScheduledTaskAnnotation) {
            if (!ScheduledTaskAnnotationUtils.isValidScheduledTaskMethod(method)) {
                throw new BeanCreationException("The bean [" + beanName + "] contains an invalid @ScheduledTask method: ["
                        + method + "]. Method should return ScheduleStatus, and have one argument of type ScheduleRunContext.");
            }
        }

        log.info(LOG_PREFIX + "Found class " + targetClass.getSimpleName() + " with "
                + methodsWithScheduledTaskAnnotation.size()
                + " @ScheduledTask methods.");

        // ?: Do we have a ScheduledTaskService object already?
        if (_scheduledTaskRegistry != null) {
            // -> Yes, this means that the context has been refreshed, and we have a reference to the service.
            // We can go ahead and register the scheduled task immediately.
            methodsWithScheduledTaskAnnotation.forEach(this::registerScheduledTaskMethod);
        }
        else {
            // -> No, the context has not been refreshed yet. We store for registration for after context refreshed.
            _methodsWithScheduledTaskAnnotations.addAll(methodsWithScheduledTaskAnnotation);
        }

        _classesThatHaveBeenChecked.add(targetClass);

        return bean;
    }

    private void registerScheduledTaskMethod(Method method) {
        synchronized (_registeredMethodsWithScheduledTaskAnnotations) {
            if (_registeredMethodsWithScheduledTaskAnnotations.contains(method)) {
                log.warn(LOG_PREFIX + "Trying to register mehtod [" + method + "] more than once.");
                return;
            }
            ScheduledTaskAnnotationUtils.registerMethod(method, _scheduledTaskRegistry, _instanceResolver);
            _registeredMethodsWithScheduledTaskAnnotations.add(method);
        }
    }

    @EventListener
    public void onContextRefreshedEvent(ContextRefreshedEvent ev) {
        ApplicationContext applicationContext = ev.getApplicationContext();
        _scheduledTaskRegistry = applicationContext.getBean(ScheduledTaskRegistry.class);

        try {
            _instanceResolver = applicationContext.getBean(ScheduledTaskInstanceResolver.class);
        }
        catch (NoSuchBeanDefinitionException ex) {
            log.info("No ScheduledTaskInstanceResolver bean found in Spring - creating default instance resolver.");
            _instanceResolver = CombinedInstanceResolver.of(
                    new SpringInstanceResolver(applicationContext),
                    new SimpleInstanceResolver());
        }

        // TODO: Add class path scanning support

        for (Method method : _methodsWithScheduledTaskAnnotations) {
            registerScheduledTaskMethod(method);
        }
        _methodsWithScheduledTaskAnnotations.clear();
    }
}
