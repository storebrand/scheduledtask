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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.context.annotation.AnnotationBeanNameGenerator;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.context.annotation.Role;
import org.springframework.core.type.AnnotationMetadata;

import com.storebrand.scheduledtask.ScheduledTaskRegistry;
import com.storebrand.scheduledtask.spring.EnableScheduledTasks.ScheduledTaskBeanRegistration;

/**
 * Spring annotation for enabling scheduled tasks. Sets up the {@link ScheduledTaskRegistryFactory} that will provide an
 * instance of the {@link ScheduledTaskRegistry}.
 * <p>
 * The factory will by default assume that we want to use the standard implementation of repositories provided by the
 * scheduledtask-db-sql package. These require a {@link javax.sql.DataSource} bean in the spring context. If there are
 * multiple beans of this type then one must be selected as a primary bean.
 * <p>
 * If you do not wish to use the standard implementation of repositories you can override this behaviour by providing
 * implementations of {@link com.storebrand.scheduledtask.db.ScheduledTaskRepository} and
 * {@link com.storebrand.scheduledtask.db.MasterLockRepository} in the Spring context.
 *
 * @author Kristian Hiim
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Import({ ScheduledTaskAnnotationRegistration.class, ScheduledTaskBeanRegistration.class })
public @interface EnableScheduledTasks {

    @Role(BeanDefinition.ROLE_INFRASTRUCTURE)
    public class ScheduledTaskBeanRegistration implements ImportBeanDefinitionRegistrar {
        private static final Logger log = LoggerFactory.getLogger(ScheduledTaskBeanRegistration.class);

        @Override
        public void registerBeanDefinitions(AnnotationMetadata importingClassMetadata,
                BeanDefinitionRegistry registry) {
            // :: Register ScheduledTaskServiceFactory bean
            log.info("Enabling Scheduled tasks - Registering Spring bean definition for "
                    + ScheduledTaskRegistryFactory.class.getSimpleName());
            BeanDefinition beanDefinition = BeanDefinitionBuilder.genericBeanDefinition(ScheduledTaskRegistryFactory.class)
                    .getBeanDefinition();
            registry.registerBeanDefinition(
                    AnnotationBeanNameGenerator.INSTANCE.generateBeanName(beanDefinition, registry), beanDefinition);
        }
    }
}
